import shutil, errno
import os

def copy_dir2dir(src, dst):
    try:
        shutil.copytree(src, dst)
    except OSError as exc: # python >2.5
        if exc.errno in (errno.ENOTDIR, errno.EINVAL):
            shutil.copy(src, dst)
        else: raise

def remove_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)