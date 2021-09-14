import multiprocessing

def _get_wanted_num_workers(num_workers):
    return  multiprocessing.cpu_count() - 1 if num_workers is None or -1 else num_workers

def mp_map(fn, iterable,  chunksize=1, num_worker=None):
    num_worker = _get_wanted_num_workers(num_worker)
    with multiprocessing.Pool(processes=num_worker) as pool:
        ret_blocking = pool.map(fn, iterable, chunksize=chunksize)
    return ret_blocking
