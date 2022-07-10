import asyncio
from typing import BinaryIO, Optional, Dict, List
import aiohttp
import aiofiles
from aiofiles import os as aioos
from aiohttp_retry import RetryClient, ExponentialRetry

import os
from retry import retry
from contextlib import suppress

# Setup logging
from mwfunctions.logger import get_logger
from mwfunctions.io.dataclasses import RequestMethod
from mwfunctions.io.exceptions import FailedRequest, NotRetryResponseStatusCode

LOGGER = get_logger(
    __name__, labels_dict={"source": "mvfunctions", "part": "async_io"}
)
ASYNC_HTTP_SESSION = None # must be initilized inside of event loop/ async def
HTTP_STATUS_CODES_TO_RETRY = [500, 502, 503, 504] # default response codes where http should be sended again

# TODO: session does not get destroyed.


class MVASession():
    """ Singleton for client creation. Omits creating the client multiple times
    for download multiprocssing. Its allowed so far from the google api to use
    this in threads. """
    session = None

    @staticmethod
    def get_cached_session():
        if not MVASession.session:
            MVASession.session = MVASession.create_session()
        return MVASession.session

    @staticmethod
    def create_session() -> aiohttp.ClientSession:
        return aiohttp.ClientSession()

def create_async_http_session(use_cache=True, connector: Optional[aiohttp.TCPConnector]=None, timeout: Optional[int]=None) -> aiohttp.ClientSession:
    """ Function to create an aiphttp Session object. Object must be initialized inside of coroutine/event loop.
        Besides async function is also sync because cannot be called async in init of a class
        TODO: Exclude one create function

        Args:
            use_cache: Tries to use cache if True
    """
    global ASYNC_HTTP_SESSION
    timeout = aiohttp.ClientTimeout(total=timeout) if timeout else aiohttp.helpers.sentinel
    # If not use cache create new session or cache does not already exists
    if not use_cache or ASYNC_HTTP_SESSION == None:
        ASYNC_HTTP_SESSION = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return ASYNC_HTTP_SESSION

async def acreate_async_http_session(use_cache=True, connector: Optional[aiohttp.TCPConnector]=None, timeout: Optional[int]=None) -> aiohttp.ClientSession:
    """ Function to create an aiphttp Session object. Object must be initialized inside of coroutine/event loop.
        Function is async to enforce developer to create session inside of async function.

        Args:
            use_cache: Tries to use cache if True
    """
    global ASYNC_HTTP_SESSION
    timeout = aiohttp.ClientTimeout(total=timeout) if timeout else aiohttp.helpers.sentinel
    # If not use cache create new session or cache does not already exists
    if not use_cache or ASYNC_HTTP_SESSION == None:
        ASYNC_HTTP_SESSION = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return ASYNC_HTTP_SESSION

async def acreate_async_retry_client(retry_options=None, *args, **kwargs) -> RetryClient:
    retry_options = retry_options or ExponentialRetry(attempts=5)
    return RetryClient(retry_options=retry_options, *args, **kwargs)

async def delete_empty_file(filepath):
    if (await aioos.stat(filepath)).st_size == 0:
        LOGGER.error(f"Removing empty file")
        await aioos.remove(filepath)


async def bytes_to_file(bytes, filepath, do_overwrite=False, exist_ok: bool = False):
    # Dups can trigger an file_exist error, if a task donwloads the file after file_exist checking
    mode = "wb" if do_overwrite else "xb"
    if exist_ok and await aioos.path.exists(filepath):
        return
    async with aiofiles.open(filepath, mode) as f:
        await f.write(bytes)

async def get_url_response(url: str,
                           session: Optional[aiohttp.ClientSession] = None, **kwargs):
    session = session if session else MVASession.get_cached_session()
    response = await session.get(url, **kwargs)
    return response

async def post_url_response(url: str,
                           session: Optional[aiohttp.ClientSession] = None, **kwargs):
    session = session if session else MVASession.get_cached_session()
    response = await session.post(url, **kwargs)
    return response

async def head_url_response(url: str,
                            session: Optional[aiohttp.ClientSession] = None, **kwargs):
    session = session if session else MVASession.get_cached_session()
    response = await session.head(url, **kwargs)
    return response

async def download_url_bytes(url: str, raise_if_status_is_not_in: Optional[List[int]]=None,
                             session: Optional[aiohttp.ClientSession] = None, **kwargs):
    response = await get_url_response(url, session=session, **kwargs)
    # Note: .raise_for_status() does not raise in case of status 302. Therefore custom raise_if_status_is_not_in param was included
    response.raise_for_status()
    if raise_if_status_is_not_in and response.status not in raise_if_status_is_not_in:
        raise ValueError(f"Status of response is {response.status} and not one of [{','.join([str(s) for s in raise_if_status_is_not_in])}]")
    return await response.read()


async def download_url_to_file(url,
                               filepath,
                               session,
                               do_overwrite: bool = False,
                               exist_ok: bool = False):
    # Only download if file does not exist or should be overwritten
    if do_overwrite or not await aioos.path.exists(filepath):
        await aioos.makedirs(os.path.dirname(filepath), exist_ok=True)
        file_bytes = await download_url_bytes(url=url, session=session)
        await bytes_to_file(bytes=file_bytes,
                            filepath=filepath,
                            do_overwrite=do_overwrite,
                            exist_ok=exist_ok)

         # If not downloaded correctly
        await delete_empty_file(filepath=filepath)

        if not await aioos.path.exists(filepath):
            raise RuntimeError(f"Could not download {url}")
    return filepath


# @retry(Exception, tries=2, delay=1)
async def download_url_to_dir(url,
                              file_name=None,
                              dest_dir=None,
                              do_overwrite: bool = False,
                              exist_ok: bool = False,
                              session=None):
    """
    file_name: If not given take basename as filename"""

    session = MVASession.get_cached_session() if not session else session
    rel_filepath = file_name if file_name else os.path.basename(url)
    curr_download_pth = f"{dest_dir}/{rel_filepath}"
    await download_url_to_file(url=url,
                               filepath=curr_download_pth,
                               session=session,
                               do_overwrite=do_overwrite,
                               exist_ok=exist_ok)
    return curr_download_pth


async def download_urls_to_filepaths(urls: List[str],
                                     filepaths: List[str],
                                     exist_ok: bool = False,
                                     do_overwrite: bool = False,
                                     session: Optional[aiohttp.ClientSession] = None,
                                     chunksize=100):
    """ returned download_paths will be INORDER """
    if len(urls) != len(filepaths):
        raise ValueError("Urls and filepaths need to have the same lenghts")

    session = session if session else MVASession.get_cached_session()

    # Chunk lists
    list_of_url_lists = [urls[i:i + chunksize]
                         for i in range(0, len(urls), chunksize)]
    list_of_filepath_lists = [filepaths[i:i + chunksize]
                              for i in range(0, len(filepaths), chunksize)]

    # Download
    download_paths_list = []
    for url_list, filepath_list in zip(list_of_url_lists, list_of_filepath_lists):
        download_futures = [download_url_to_file(
            url, filepath=filepath, exist_ok=exist_ok, do_overwrite=do_overwrite, session=session) for url, filepath in zip(url_list, filepath_list)]
        download_paths = await asyncio.gather(*download_futures, return_exceptions=True)
        download_paths_list += download_paths

    # care for errors in download_paths_list

    # TODO: check if this is a flattend list
    return download_paths_list


async def download_urls_to_dir(urls: List[str],
                               dest_dir: str,
                               filenames: Optional[List[str]],
                               session: Optional[aiohttp.ClientSession] = None,
                               do_overwrite: bool = False,
                               chunksize=100):
    """ returned download_paths will be INORDER """
    list_of_filename_lists = None
    if filenames is not None:
        if len(urls) != len(filenames):
            raise ValueError(
                "Urls and filenames need to have the same lenghts")
        list_of_filename_lists = [filenames[i:i + chunksize]
                                  for i in range(0, len(filenames), chunksize)]
    session = session if session else MVASession.get_cached_session()
    list_of_url_lists = [urls[i:i + chunksize]
                         for i in range(0, len(urls), chunksize)]
    download_paths_list = []

    # TODO: can probably simplified
    if list_of_filename_lists:
        for url_list, filename_list in zip(list_of_url_lists, list_of_filename_lists):
            download_futures = [download_url_to_dir(
                url, dest_dir=dest_dir, file_name=filename, do_overwrite=do_overwrite, exist_ok=True, session=session) for url, filename in zip(url_list, filename_list)]
            download_paths = await asyncio.gather(*download_futures)
            download_paths_list.append(download_paths)
    else:
        for url_list in list_of_url_lists:
            download_futures = [download_url_to_dir(
                url, dest_dir=dest_dir, do_overwrite=do_overwrite, session=session) for url in url_list]
            download_paths = await asyncio.gather(*download_futures)
            download_paths_list.append(download_paths)

    # TODO: check if this is a flattend list
    return download_paths_list

    # print('Results')
    # for download_future in asyncio.as_completed(download_futures):
    #     result = yield from download_future
    #     print('finished:', result)
    # return urls

from functools import partial

async def download_urls_to_filepaths_apply(
    urls: List[str],
    filepaths: List[str],
    do_overwrite: bool = False, 
    exist_ok: bool = False,
    num_worker: int = -1,
    chunksize: int = 1000,
):
    """Inorder multiprocessing download.

    Args:
        gs_urls (List[str]): [description]
        dest_dir (str): [description]
        no_globbing (bool): [description]
        verbose (int, optional): [description]. Defaults to 0.
        num_worker (int, optional): [description]. Defaults to -1.

    Returns:
        list: Downloaded path, None if error.
    """
    from mvfunctions.profiling import log_time
    from mvfunctions.parallel import a_mp_map
    from tqdm import tqdm
    from mvfunctions.io import get_num_worker

    # from mvfunctions import environment

    # TODO: do i download if the file already exist?

    list_of_url_lists = [urls[i:i + chunksize]
                         for i in range(0, len(urls), chunksize)]
    list_of_filepath_lists = [filepaths[i:i + chunksize]
                              for i in range(0, len(filepaths), chunksize)]

    async with aiohttp.ClientSession() as session:
        download_fn = partial(
            download_url_to_file,
                                     exist_ok = exist_ok,
                                     do_overwrite = do_overwrite,
                                     session = session)
                                     # chunksize=100)




        with log_time("Donwloading async on single thread"):
            pbar = tqdm(total=len(urls), desc="Download")
            
            ret_list = []
            for list_of_urls, list_of_filepaths in zip(list_of_url_lists, list_of_filepath_lists):
                tmp_ret_list = await asyncio.gather(*list(map(download_fn, list_of_urls, list_of_filepaths)))
                ret_list += tmp_ret_list
                pbar.update(len(list_of_urls))

            # with log_time("a_mp_map"):
            #     ret_list = await a_mp_map(
            #         download_fn,
            #         urls, filepaths,
            #         chunksize=chunksize,  # leave it for 1 for now
            #         num_worker=int(num_worker))

            # paths, error_paths = zip(*ret_list)
            # paths = [item for sublist in paths for item in sublist]
            # errors = [bool(len(item)) for item in error_paths]
            # paths = [path if not error else None for path, error in zip(paths, errors)]

    return ret_list

def exponential_backoff(wait_time_before, backoff_factor):
    return wait_time_before * backoff_factor

async def send_and_retry_http(url: str,
                              method: RequestMethod,
                            retries: int=1,
                            init_wait_time: float=0.5, # half a second
                            backoff: float=3,
                            http_status_codes_to_retry=HTTP_STATUS_CODES_TO_RETRY,
                            session: aiohttp.ClientSession = None,
                            **kwargs):
    """
    Sends a aio HTTP request and implements a retry logic.
    implements exponential backoff (No exp backoff if backoff is 1).

    Arguments:
        session (obj): A client aiohttp session object
        method (str): Method to use
        url (str): URL for the request
        retries (int): Number of times to retry in case of failure
            -1: indefinitly
            0: Try only once
            n: Try n times after failure
        init_wait_time (float): Time to wait before retries in seconds
        backoff (int): Multiply interval by this factor after each failure
        read_timeout (float): Time to wait for a response
    """

    if method not in RequestMethod.to_list():
        raise ValueError(f"method {method} is not part of {RequestMethod.to_list()}")

    session = session if session else create_async_http_session(use_cache=True)
    retry_attempts = 0 # starts with 0 attempts
    wait_time = init_wait_time # init wait time with "init_wait_time"
    raised_exc: Optional[FailedRequest] = None # custom exception of aiohttp failed request

    while retry_attempts <= retries:
        # if we got a
        if raised_exc:
            LOGGER.error(f"{raised_exc.get_error_msg()} | remaining tries: {retries-retry_attempts} sleeping: {wait_time}s")
            await asyncio.sleep(wait_time)
        try:
            # TODO: Make sure session is open
            if session.closed:
                raise NotImplementedError

            async with getattr(session, str(method))(url, **kwargs) as response:
                if response.status == 200:
                    try:
                        json_data = await response.json()
                    except Exception as exc:
                        raise ValueError(f"Json load failed due to {type(exc)}")
                    # SUCCESSFULL RESPONSE
                    return json_data, response

                elif response.status in http_status_codes_to_retry:
                    LOGGER.error(f'received invalid response code:{response.status} url:{url} response:{response.reason}')
                    raise aiohttp.http_exceptions.HttpProcessingError(
                        code=response.status)
                else:
                    raise NotRetryResponseStatusCode(f"Got Response with status code {response.status}, but request is only retried if status code is one of {http_status_codes_to_retry}")
        except NotRetryResponseStatusCode as exc:
            # TODO: What shoul happen in case response code is not part of http_status_codes_to_retry?
            LOGGER.error(f"{exc}")
            raise exc
        except Exception as exc:
            status_code=None
            with suppress(AttributeError):
                # try to get status code out of exception
                status_code = exc.code # HttpProcessingError contains status code for example
            raised_exc = FailedRequest(status_code=status_code, message=str(exc), url=url,
                                       raised=exc.__class__.__name__)

        # increment attempts and increase wait_time
        retry_attempts += 1
        wait_time = exponential_backoff(wait_time, backoff)

    if raised_exc:
        raise raised_exc




