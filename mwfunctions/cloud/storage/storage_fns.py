import os
from google.cloud import storage
from google.cloud.storage.blob import Blob
from typing import Optional, Union, Iterator, Tuple, List, BinaryIO
from functools import partial
import pandas as pd

import json

import tqdm
from mwfunctions.exceptions import log_suppress, log_if_except
import mwfunctions.misc

GCS_CLIENT = storage.Client()


# Setup logging
from mwfunctions.logger import get_logger
LOGGER = get_logger(__name__, labels_dict={})


class StorageParams():
    def __init__(self, gs_url, project="mba-pipeline", credentials=None):
        assert "gs://" in gs_url, "not a valid gs_url"

        self.project = project
        self.gs_url = gs_url
        self.bucket_name = self.gs_url.split("gs://")[1].split("/")[0]
        self.path = "/".join(self.gs_url.split("gs://")[1].split("/")[1:])
        self.credentials = credentials

def read_file_as_string(gs_url):
    storage_params = StorageParams(gs_url)
    bucket = GCS_CLIENT.get_bucket(storage_params.bucket_name)
    blob = bucket.get_blob(storage_params.path)
    return blob.download_as_string()

def read_json_as_dict(gs_url):
    return json.loads(read_file_as_string(gs_url))


def read_file(path) -> BinaryIO:
    """ Reads file from local or gcs. Returns a file pointer (fp). You need to call .read() to get the bytes """
    import io
    import requests
    if path[:5] == "gs://":
        file_bytes = read_file_as_bytes(path)
        fp = io.BytesIO(file_bytes)
    elif path[:4] == "http":
        fp = requests.get(path, stream=True).raw
    else:
        fp = open(path, "rb")

    return fp

def read_file_as_bytes(gs_url,
             max_retries: int = 2,
             suppress_exception=False):
    """ Downloads from gs_url returns a bytes object for a file.
    """
    error_filepaths = []
    blob = Blob.from_string(gs_url, client=GCS_CLIENT)
    except_context = log_suppress if suppress_exception else log_if_except

    fn = partial(blob.download_as_bytes, client=GCS_CLIENT)
    with except_context("Could not download: {}".format(gs_url), Exception):
        return mwfunctions.misc.do_retry_if_exeption(fn, max_retries=max_retries)

    download_paths = [blob.name for blob in blobs]
    return download_paths, error_filepaths


def download(gs_url,
             destination_dir=None,
             do_overwrite: bool = False,
             max_retries: int = 2,
             storage_client: storage.Client = None,
             force_as_file=False,
             with_rel_path=False,
             suppress_exception=False,
             log_verbose=False):

    #with log_time("Create client", is_on=False):
    client = GCS_CLIENT
    # if log_verbose:
    #     print(client)

    # with log_time("Rest of download", is_on=False):
    blobs = []
    error_filepaths = []
    gs_splits = gs_url.split('/')
    bucket_name = gs_splits[2]
    prefix = '/'.join(gs_splits[3:])
    # A file has . in it
    if "." in os.path.basename(gs_url) or force_as_file:
        # with log_time("As file", is_on=True):
        blob = Blob.from_string(gs_url, client=client)
        # if blob.exists(client=client):  # this is a class A Operation and should be skipped
        blobs.append(blob)
    # Clone dir
    else:
        assert destination_dir is not None, "You have to provide a download_path to download a dir"
        LOGGER.warning("Costly class A Operation!")
        blobs = list(client.list_blobs(bucket_or_name=bucket_name, prefix=prefix))  # Get list of files

    # if len(blobs) == 0:
    #     raise FileNotFoundError("No files found for gs_url {}".format(gs_url))

    except_context = log_suppress if suppress_exception else log_if_except

    # if no destination dir -> download as file
    if destination_dir is None and len(blobs) == 1:
        blob = blobs[0]
        fn = partial(blob.download_as_bytes, client=client)
        with except_context("Could not download: {}".format(gs_url), Exception):
            return mwfunctions.misc.do_retry_if_exeption(fn, max_retries=max_retries)
    else:
        # donwload_paths = []
        # Disable pbar for only 1 blob
        pbar_disabled = len(blobs) == 1 or not log_verbose
        with tqdm.tqdm(total=len(blobs), desc="Downloading: ", disable=pbar_disabled) as pbar:
            for blob in blobs:
                # filename = blob.name.replace(prefix, os.path.basename(prefix))
                rel_filepath = blob.name if with_rel_path else blob.name.replace(prefix, os.path.basename(prefix))
                curr_download_pth = os.path.join(destination_dir, rel_filepath)
                # Put in list
                # donwload_paths.append(curr_download_pth)
                if do_overwrite or not os.path.exists(curr_download_pth):
                    os.makedirs(os.path.dirname(curr_download_pth), exist_ok=True)
                    if log_verbose:
                        LOGGER.info(f"Downloading: {gs_url} to {curr_download_pth}")
                    pbar.set_description_str(f"Downloading: {blob.name}")
                    fn = partial(blob.download_to_filename, filename=curr_download_pth, client=client)
                    # with log_time("Acutal download", is_on=True):
                    with except_context(f"Could not download: {gs_url}", Exception):
                        mwfunctions.misc.do_retry_if_exeption(fn, max_retries=max_retries)
                    # Test not empty
                    if os.stat(curr_download_pth).st_size == 0:
                        LOGGER.error(f"Removing empty file")
                        os.remove(curr_download_pth)
                    # Check if the downloaded file exists, else log the error
                    if not os.path.exists(curr_download_pth):
                        error_filepaths.append(curr_download_pth)
                else:
                    if log_verbose:
                        LOGGER.info(
                            f"The download_file_path: {curr_download_pth} already exists")
                pbar.update(1)
        # return download_paths

    download_paths = [blob.name for blob in blobs]
    return download_paths, error_filepaths


def upload_filebytes(bucket, file_path, file_bytes,
                     content_type=None, max_retries=10):
    """
    bucket: a bucket object from gc
    file_path: path without the bucket_name

    """
    # Create a blob
    blob = bucket.blob(blob_name=file_path)
    i = 0
    while True:
        try:
            blob.upload_from_string(data=file_bytes, content_type=content_type)
            return blob.time_created, blob.size
        except Exception as e:
            if i == max_retries:
                LOGGER.critical("Could not upload: %s", file_path)
                exit(-1)
            LOGGER.error("Could not upload, trying again")
            i += 1
            time.sleep(0.1)


def upload(gs_url,
           path=None,
           file_bytes=None,
           content_type=None,
           max_retries: int = 10,
           do_overwrite: bool = True,
           force_as_file=False,
           storage_client: storage.Client = None,
           timeout=300,
           suppress_exception=False):
    assert bool(path) != bool(file_bytes), "Either you provide a path or filebytes"

    client = GCS_CLIENT
    if path:
        path = path.replace("\\", "/")
    gs_url = gs_url[:-1] if gs_url[-1] == '/' else gs_url
    except_context = log_suppress if suppress_exception else log_if_except

    if file_bytes:
        assert "." in gs_url or force_as_file, "You have to provide an filepath with an extension or set force_as_file"
        blob = Blob.from_string(gs_url, client=client)
        if not do_overwrite and blob.exists(client=client):
            LOGGER.warning(
                "Upload url already exists: {}".format(gs_url))
            return
        fn = partial(blob.upload_from_string, data=file_bytes, content_type=content_type, client=client, timeout=timeout)
        LOGGER.info(f"Uploading file_bytes to {gs_url}")
        with except_context(f"Could not upload: {gs_url}", Exception):
            mvfunctions.misc.do_retry_if_exeption(fn, max_retries=max_retries)

    # We have dirs/files on storage
    else:
        loc_filepaths = []
        gcs_path_appends = []
        if os.path.isfile(path) or force_as_file:
            loc_filepaths.append(path)
            gcs_path_appends.append(os.path.basename(path))
        # We have a directory. We need to get the directory name and get everything in it
        # the directory and every file in it will be uploaded in that structure
        else:
            del_prefix_left_len = len('/'.join(path.split('/')[:-1]))
            root_dir_name = path.split('/')[-1]  # everyting except the dir which should be uploaded
            # files = glob.glob(path +"/**", recursive=True)
            for root, dirs, files in os.walk(path):
                for file in files:
                    loc_filepath = os.path.join(root, file).replace("\\", "/")
                    gcs_append = loc_filepath[del_prefix_left_len:]
                    loc_filepaths.append(loc_filepath)
                    gcs_path_appends.append(gcs_append)

        pbar_disabled = False
        for loc_filepath, gcs_append in tqdm.tqdm(zip(loc_filepaths, gcs_path_appends), desc="Uploading: ", disable=pbar_disabled):
            to_gsurl = f"{gs_url}{gcs_append}"
            blob = Blob.from_string(to_gsurl, client=client)
            # Costly Class A Operation if do_overwrite is False
            if not do_overwrite and blob.exists(client=client):
                LOGGER.warning(
                    "Upload url already exists: {}".format(to_gsurl))
                continue
            # file_path = os.path.join(local_path, loc_filepath)
            LOGGER.info("Uploading {} to {}".format(loc_filepath, to_gsurl))
            fn = partial(blob.upload_from_filename, filename=loc_filepath, client=client, timeout=timeout)
            with except_context("Could not upload: {}".format(to_gsurl), Exception):
                mwfunctions.misc.do_retry_if_exeption(fn)


def download_apply(gs_urls: List[str], dest_dir: str, no_globbing: bool, verbose: int = 0, num_worker: int = -1,
                   chunksize: int = 1):
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
    from mwfunctions.parallel import mp_map
    from tqdm import tqdm
    from mwfunctions import environment

    download_fn = partial(download,
                          destination_dir=dest_dir,
                          do_overwrite=not no_globbing,
                          force_as_file=True,
                          with_rel_path=True,
                          suppress_exception=True,
                          log_verbose=verbose)

    # assert num_worker <= len(gs_urls)
    cpu_count = environment.get_cpu_count()
    if num_worker == -1 or num_worker > len(gs_urls) or cpu_count:
        num_worker = min(len(gs_urls), cpu_count)

    gs_urls = tqdm(gs_urls, desc="File download", )
    ret_iter = map(download_fn, gs_urls) if num_worker == 1 else mp_map(download_fn,
                                                                        gs_urls,
                                                                        chunksize=chunksize,
                                                                        # leave it for 1 for now
                                                                        num_worker=int(num_worker))  # bug?

    paths, error_paths = zip(*ret_iter)
    paths = [item for sublist in paths for item in sublist]
    errors = [bool(len(item)) for item in error_paths]
    # paths = [path if not error else None for path, error in zip(paths, errors)]

    return paths, errors

def get_gcs2LocalJsonGen(json_path_df: pd.DataFrame, local_output_dir: str, json_gs_url_col="json_gs_url"):
    """ Download json files containing the objects and provide access as generator.

    Filter for a category, this will merge the dataset_table with the json table and atm selects "main_category" as
    the column to use to filter.
    """
    # gs_urls = [gs_url[0] for gs_url in json_path_df[bmc.JSON_GS_URL_COL].values.tolist()]  # Flatten the list
    gs_urls = json_path_df[json_gs_url_col].values.tolist()
    gcs2LocalJsonGen = GCS2LocalJsonGen(gs_urls, local_output_dir=local_output_dir)
    assert len(gcs2LocalJsonGen) == len(json_path_df), "Lens not euqal...."
    return gcs2LocalJsonGen, json_path_df

class GCS2LocalJsonGen(object):
    def __init__(self,
                 gs_urls: List[str],
                 local_output_dir: str):
        """ Downloads json files specified in the given bigquery table.

        WARNING:
            Output order not the same as input gs_urls

        TODO: What is with memory? Both if we do

        """

        self.gs_urls = gs_urls
        self.max_elems = len(self.gs_urls)
        self.local_output_dir = local_output_dir

        self.download_jsons()

        # Get a list of the local files
        self.local_json_files = self.list_local_json_files()
        self.idx = 0

    def download_jsons(self, do_overwrite=False):
        """
        Temporarily download the df, extract the json paths and donwload them.

        TODO:
            - Maybe hardcode category_col

        :param do_overwrite:
        :return:
        """
        # Startup of mp take to long if we only have a small amount of files
        singleprocess = False # if self.max_elems < 5 else False

        self.local_paths, errors = download_apply(self.gs_urls, dest_dir=self.local_output_dir, no_globbing=~do_overwrite, verbose=False, num_worker=1 if singleprocess else -1)

    def list_local_json_files(self):
        """ """
        return [f"{self.local_output_dir}/{file}" for file in self.local_paths]
        # return [os.path.join(self.local_output_dir, file) for file in os.listdir(self.local_output_dir)]

    def __iter__(self):
        self.idx = 0
        return self

    def __len__(self):
        return len(self.local_json_files)

    def __next__(self):
        try:
            while self.max_elems >= self.idx:
                json_file = self.local_json_files[self.idx]
                try:
                    self.idx += 1
                    with open(json_file) as f:
                        return json.load(f)
                except json.JSONDecodeError as e:
                    LOGGER.error(f"Could not decode json: {json_file}")
        except IndexError:
            pass
        # except IndexError:
        #     raise StopIteration
        # finally:
        # Important to condisder mistakes from manually taking next and maybe skipping an element
        # self.idx = 0
        raise StopIteration
