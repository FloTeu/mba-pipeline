import contextlib
import time

# prepare logger
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

@contextlib.contextmanager
def log_time(fn_name, is_on=True, log_start=True, custom_logger=None):
    if is_on:
        if log_start:
            if custom_logger:
                custom_logger.info("Processing: " + fn_name)
            else:
                print("Processing: " + fn_name)
        start = time.time()
        yield
        end = time.time()
        msg = "Processing: " + fn_name + ' finished in ' + str(end - start) + 's'
        if custom_logger:
            custom_logger.info(msg)
        else:
            print(msg)
    else:
        pass
        yield
        pass


def get_memory_used_in_gb():
    import os, psutil
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024 / 1024

@contextlib.contextmanager
def log_memory(fn_name, is_on=True):
    if is_on:
        print("Processing: " + fn_name, "with memory in gb %.2f" % get_memory_used_in_gb())
        yield
        print("Processing: " + fn_name + " finished with memory in gb %.2f" % get_memory_used_in_gb())
    else:
        pass
        yield
        pass


