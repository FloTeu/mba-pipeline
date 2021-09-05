import time

from functools import wraps

def print_time_elapsed(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        ts = time.time()
        response = await func(*args, **kwargs)
        print("Elapsed time for handling request: %.2fs" % (time.time() - ts))
        return response
        
    return wrapper

def makebold2(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        print("MAKE BOLD")
        return "<b>" + fn(*args, **kwargs) + "</b>"
    return wrapper

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print ('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result
    return timed