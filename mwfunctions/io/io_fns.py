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


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        # maybe argparse.ArgumentTypeError('Boolean value expected.') must be raised in case of argparse
        raise ValueError("Provided argument is not a bool")