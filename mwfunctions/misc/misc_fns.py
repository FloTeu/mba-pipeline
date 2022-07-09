import logging
import time
import traceback
from mwfunctions.logger import get_logger
LOGGER = get_logger(__name__)

def do_retry_if_exeption(fn, max_retries=10, logger=None):
    # with log_time("logger creation in do_retry_if_exeption"):
    logger = logger or LOGGER
    for i in range(max_retries):
        try:
            # with log_time("Partialized fn"):
            return fn()
        except Exception as e:
            logger.error("Cought exception: {}".format(str(e)))
            logger.error(traceback.format_exc())
            logger.error("Trying again: {}/{}".format(i, max_retries))
            time.sleep(0.1)
    fn_name = "fn.__name__ attribute not set, probably a partial function"
    with suppress(Exception):
        fn_name = fn.__name__
    raise RuntimeWarning("Reached max retries for function %s", fn_name)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]