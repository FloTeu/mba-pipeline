import argparse
import subprocess
import sys
from tempfile import mkstemp
from shutil import move, copymode
from os import fdopen, remove
import datetime
import time
import os



def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def replace(file_path, pattern, subst):
    #Create temp file
    fh, abs_path = mkstemp()
    with fdopen(fh,'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(pattern, subst))
    #Copy the file permissions from the old file to the new file
    copymode(file_path, abs_path)
    #Remove original file
    remove(file_path)
    #Move new file
    move(abs_path, file_path)

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--use_public_proxies', type=str2bool, nargs='?', const=False)
    parser.add_argument('--use_private_proxies', type=str2bool, nargs='?', const=False)

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    use_public_proxies = args.use_public_proxies
    use_private_proxies = args.use_private_proxies
    print(os.getcwd())

    if use_public_proxies:
        replace("mba_crawler/settings.py", "use_public_proxies = False", "use_public_proxies = True")
    else:
        replace("mba_crawler/settings.py", "use_public_proxies = True", "use_public_proxies = False")
    if use_private_proxies:
        replace("mba_crawler/settings.py", "use_private_proxies = False", "use_private_proxies = True")
    else:
        replace("mba_crawler/settings.py", "use_private_proxies = True", "use_private_proxies = False")

    # change settings for us 
    if marketplace == "de":
        replace("mba_crawler/settings.py", "only_usa = True", "only_usa = False")
    if marketplace == "com":
        replace("mba_crawler/settings.py", "only_usa = False", "only_usa = True")
            
if __name__ == '__main__':
    main(sys.argv)
