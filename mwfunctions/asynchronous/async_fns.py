import asyncio
import functools
import itertools

from typing import Callable
from functools import wraps
from enum import Enum
from typing import Any, Optional, Callable, List, Coroutine, Set, Union, Awaitable, Iterable, Tuple, Dict
from mwfunctions.misc import chunks

"""
### Helpful Notes
    Coroutine:  Every async function returns a Coroutine object. The function is not executed immediately but scheduled and executed in parallel with other coroutines later.
                The execution of a coroutine depends on development. For example asyncio.gather(*coroutines) can be used to generate a Future object out of multiple coroutines. 
                Future objects can than be executed in parallel by an event loop on OS thread.

    Task:       Subclass of Future. Can be created by single coroutine and has state which starts with "PENDING" and ends with "FINISHED". 
                Tasks can contain also callbacks which are triggered after task is finished.
                Tasks are not executed immediately and must be scheduled by event loop.

    Future:     A Future represents an eventual result of an asynchronous operation. Not thread-safe. (ayncio docu)
                Futures of asyncio are not executed immediately and only prepare parallel execution of awaitables (e.g. coroutines or tasks).
                Response can be fetched later. 
        
    Known Errors:
    "RuntimeError: This event loop is already running":
        Error happens if process already running on event loop e.g. (FastAPI, jupyter notebook etc.) https://medium.com/@vyshali.enukonda/how-to-get-around-runtimeerror-this-event-loop-is-already-running-3f26f67e762e
        Solution: 
            Install nest-asyncio which allows nested loops if loop is already blockes by service like FastAPI
            `pip3 install nest-asyncio
            import nest_asyncio
            nest_asyncio.apply()`
            
"""


####### from vespa
def _run_coroutine_new_event_loop(loop, coro):
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

def check_for_running_loop_and_run_coroutine(coro):
    try:
        _ = asyncio.get_running_loop()
        new_loop = asyncio.new_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    _run_coroutine_new_event_loop, new_loop, coro
                )
                return_value = future.result()
        return return_value
    except RuntimeError:
        return asyncio.run(coro)



class TaskState(str, Enum):
    FINISHED="FINISHED"
    PENDING="PENDING"

def get_new_event_loop():
    return asyncio.new_event_loop()

def create_new_event_loop():
    asyncio.set_event_loop(get_new_event_loop())

# TODO: asyncio.unix_events._UnixSelectorEventLoop not found in Version python 3.9
def get_event_loop(create_if_not_exists=True): # -> asyncio.unix_events._UnixSelectorEventLoop:
    """ Prevent error in a different then main thread as asyncio.get_event_loop creates only in main thread """
    # gets current event loop set in current OS thred. If nothing exists a new loop is created in current OS thread
    try:
        return asyncio.get_event_loop()
    except RuntimeError as e:
        if create_if_not_exists and "There is no current event loop" in str(e):
            create_new_event_loop()
            return asyncio.get_event_loop()


def aws2future(awaitables: Union[List[Awaitable], Awaitable], return_exceptions=False, create_event_loop_if_not_exists=True):# -> asyncio.tasks._GatheringFuture:
    """ Returns a _GatheringFuture (child of asyncio.Future) which contains all coroutines/awaitables and is not executud immediately and fetched later on.
        Note: This function is not time consuming.
        Note: aws stands for awaitable

    :param awaitables: can be a List or set or single object of awaitables for example Coroutine or asyncio.Task
    :return: asyncio.Future
    """
    # make sure awaitables is an iteratable
    awaitables = awaitables if isinstance(awaitables, Iterable) else [awaitables]
    try:
        return asyncio.gather(*awaitables, return_exceptions=return_exceptions)
    except RuntimeError as e:
        if create_event_loop_if_not_exists and "There is no current event loop" in str(e):
            create_new_event_loop()
            return asyncio.gather(*awaitables, return_exceptions=return_exceptions)

def get_future_response(future: asyncio.Future) -> List[Any]:
    loop = get_event_loop()
    return loop.run_until_complete(future)

def get_awaitables_response(awaitables: Union[List[Awaitable], Awaitable]):
    resp = get_future_response(aws2future(awaitables))
    return resp if isinstance(awaitables, Iterable) else resp[0]

def get_coroutines_response_with_callbacks(coroutines: Union[List[Coroutine],Coroutine], done_callback_fns: Optional[List[Callable]]=None) -> Union[List[Any], Any]:
    """ Transforms coroutines to tasks to allow adding callback functions
    """
    if done_callback_fns:
        assert len(coroutines) == len(done_callback_fns), "Every coroutine needs a callback function"
    tasks = []
    # make iteratable if not already iteratable
    # TODO: decide whether function should also be able to return single object (not list of responses)
    coroutines = convert_to_iterable(coroutines)

    for i, cor in enumerate(coroutines):
        tasks.append(add_task_to_event_loop(cor, done_callback_fn=done_callback_fns[i] if done_callback_fns else None))

    response = execute_tasks(tasks)
    return response if isinstance(coroutines, Iterable) else response[0]

def get_coroutines_response(coroutines: Union[List[Coroutine],Coroutine], chunk_size: Optional[int]=None) -> Union[List[Any], Any]:
    """ This function executes all coroutines and returnes the response of each coroutine in a list

    # TODO: decide whether function should also be able to return single object (not list of responses)
    loop docu: https://docs.python.org/3/library/asyncio-eventloop.html
    :param coroutines: List of coroutines which are created by e.g. async function
    :param chunk_size: If set, coroutines are splitted into chunks and processed by max chunk_size in parallel
    :return: List of Any response of given coroutines (is also a list if single coroutine is provided)
    """
    response_list = []
    if chunk_size == None or not isinstance(coroutines, Iterable) or len(coroutines) <= chunk_size:
        response_list = get_future_response(aws2future(coroutines))
    else:
        for coroutines_chunk in chunks(coroutines, chunk_size):
            response_list.extend(get_future_response(aws2future(coroutines_chunk)))
    return response_list if isinstance(coroutines, Iterable) else response_list[0]

def add_task_to_event_loop(coroutine: Coroutine, done_callback_fn: Optional[Callable]=None, loop=None) -> asyncio.Task:
    """
    :param done_callback_fn: Function which takes a finished (_state=="FINISHED") asyncio.Task as input parameter
    :return:
    """
    loop = loop if loop else get_event_loop()
    task: asyncio.Task = loop.create_task(coroutine)
    if done_callback_fn:
        task.add_done_callback(done_callback_fn)
    return task

def execute_tasks(tasks: List[asyncio.Task], chunk_size: Optional[int]=None) -> List[Any]:
    """ Takes a list of tasks and executes all until last one is finished.
        Callback (done_callback_fn) is only called first time a task is executed.
    :param tasks:  tasks can also be a set of tasks
    TODO: chunk_size seems to have no effect, since all tasks in PENDING state are executed even if they are not in first chunk
    :param chunk_size: Optional max number of tasks per chunk. If None, all tasks are executed in parallel.
    :return: Returns any output of all tasks provided
    """
    response_list = []
    if chunk_size == None or len(tasks) <= chunk_size:
        response_list = get_future_response(aws2future(tasks))
    else:
        for tasks_chunk in chunks(tasks, chunk_size):
            response_list.extend(get_future_response(aws2future(tasks_chunk)))
    return response_list

async def await_tasks(tasks: List[asyncio.Task]) -> List[Any]:
    """ Must be called with await
        ignores unsolved pending tasks (which should not exists, since "ALL_COMPLETED" input)
        Returns Set of Tasks which have status finished
    """
    #done, pending = await asyncio.wait(tasks, return_when="ALL_COMPLETED")
    #task_set = next(iter(response))
    return await aws2future(tasks)

async def await_coroutines(coroutines: List[Coroutine], loop=None) -> List[Any]:
    """ Must be called with await
        Returns Set of Tasks which have status finished
    """
    return await await_tasks([add_task_to_event_loop(cor, loop=loop) for cor in coroutines])

def get_all_tasks() -> Set[asyncio.Task]:
    # Note: use asyncio.Task.all_tasks() instead of asyncio.all_tasks() because asyncio.all_tasks() throws an error if event loop is not running
    return asyncio.Task.all_tasks()

def get_pending_tasks() -> List[asyncio.Task]:
    return [task for task in asyncio.Task.all_tasks() if task._state == TaskState.PENDING]

def execute_all_tasks_in_event_loop(chunk_size: Optional[int]=None):
    return execute_tasks(get_all_tasks(), chunk_size=chunk_size)

def execute_pending_tasks_in_event_loop(chunk_size: Optional[int]=None):
    return execute_tasks(get_pending_tasks(), chunk_size=chunk_size)

def close_event_loop():
    loop = get_event_loop()
    loop.close()

def stop_event_loop():
    loop = get_event_loop()
    loop.close()

def is_awaitable(aw) -> bool:
    # returnes whether input is awaitable like Coroutine or Future
    return isinstance(aw, Awaitable)

def convert_to_iterable(iterable_or_not_iterable: Union[Iterable, object]) -> Iterable: # Iterable or No-Iterable
    """ If not iterable, object becomes iterable
    """
    if not isinstance(iterable_or_not_iterable, Iterable):
        return [iterable_or_not_iterable]
    return iterable_or_not_iterable

# FastApi in production funktioniert damit nicht. Da uvloop damit nicht funktioniert
# def allow_nested_event_loops():
#     # allows using async task execution within currently running event loop of fast API or other event loop using services
#     import nest_asyncio
#     nest_asyncio.apply()

import concurrent.futures
# Code copied by pyvespa. Might be useful for our async function
def _check_for_running_loop_and_run_coroutine(cor: Coroutine):
    try:
        _ = asyncio.get_running_loop()
        new_loop = asyncio.new_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            asyncio.set_event_loop(new_loop)
            future = executor.submit(
                new_loop.run_until_complete(cor)
            )
            return_value = future.result()
            return return_value
    except RuntimeError:
        return asyncio.run(cor)

def nested_async_shield(func):
    """ TODO: Not finished right now. Unclear how to use it
    """
    import asyncio
    from mvfunctions.asynchronous import async_fns

    @wraps(func)
    def async_shield(*args, **kwargs):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Create an event loop
            async_fns.get_event_loop()
        else:
            raise RuntimeError("Loop already running, use async functions")
        return func(*args, **kwargs)

    return async_shield

async def sync_function2futures(sync_fns: Callable, args_list: List[Union[Iterable[Any], Any]]=[], kwargs_list: List[Dict[str, Any]]=[]) -> List[asyncio.Future]:
    """ Function to execute sync functions parallel async

    Note: If args_list has no corresponding value in kwargs_list, value is replaced by None and vice versa.

    Args:
        sync_fns: Syncron function which should be executed in parallel async
        args_list: List of [Iterable (e.g. Tuple, List etc.)] function arguments. Index of list is shared with corresponding kwargs_list value
        kwargs_list:  List of function keyword argument dicts. Index of list is shared with corresponding args_list value

    Returns: A List of futures which (after being awaited) return output of sync_fns
    """
    loop = get_event_loop()
    future_list = []
    # if args_list has different length than kwargs_list, longest list is still iterated and other lists values become None
    for args, kwargs in itertools.zip_longest(args_list, kwargs_list):
        kwargs = kwargs if kwargs != None else {}
        # make args an iteratable if its not already
        args = args if isinstance(args,(List, Tuple)) else [args]
        # at the moment of future generation sync function is executed (but not awaited)
        future = loop.run_in_executor(None,
                                       functools.partial(sync_fns, *args if args else None, **kwargs))
        future_list.append(future)
    return future_list


# async def async_execute_sync_function(sync_fns: Callable, args_list: List[Union[Iterable[Any], Any]]=[], kwargs_list: List[Dict[str, Any]]=[]) -> List[Any]:
#     """ Function to execute sync functions parallel async
#
#         Returns: A List of responses which is returned by sync_fns
#     """
#     future_list = await sync_function2futures(sync_fns, args_list=args_list, kwargs_list=kwargs_list)
#     return [await future for future in future_list]


async def async_execute_sync_function(sync_fns: Callable, args_list: List[Union[Iterable[Any], Any]] = [],
                                      kwargs_list: List[Dict[str, Any]] = []) -> List[Any]:
    """ Function to execute sync functions parallel async

        Returns: A List of responses which is returned by sync_fns
    """
    future_list = await sync_function2futures(sync_fns, args_list=args_list, kwargs_list=kwargs_list)
    return await asyncio.gather(*future_list)



