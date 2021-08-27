import argparse
import subprocess
import sys
from tempfile import mkstemp
from shutil import move, copymode
from os import fdopen, remove
import datetime
import time
import os
import random

DAY, NIGHT = 1, 2
MARKETPLACES = ["com", "de"]

def check_time(time_to_check, on_time, off_time):
    if on_time > off_time:
        if time_to_check > on_time or time_to_check < off_time:
            return NIGHT, True
    elif on_time < off_time:
        if time_to_check > on_time and time_to_check < off_time:
            return DAY, True
    elif time_to_check == on_time:
        return None, True
    return None, False

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
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk", "all"', type=str)
    parser.add_argument('--number_products', default=10, type=int, help='Number of products/shirts that shoul be crawled. If -1, every image that is not already crawled will be crawled.')
    parser.add_argument('--proportion_priority_low_bsr_count', default=0, type=float, help='50% is the default proportion what means 50% should be design which were crawled least often')
    parser.add_argument('--repeat', default=1, type=int, help='If crawling should be repeated')
    parser.add_argument('--force_exec', default=0, type=int, help='If crawling should start at any time')
    parser.add_argument('--general_crawling_after_n_iter', default=0, type=int, help='After how many iteration steps general product crawler should start periodically. Defaults 0 -> no general crawling')

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    marketplace_rotation = False
    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    if marketplace == "all":
        marketplace_rotation = True
    repeat = args.repeat
    number_products = args.number_products
    force_exec = args.force_exec
    general_crawling_after_n_iter = args.general_crawling_after_n_iter
    proportion_priority_low_bsr_count = args.proportion_priority_low_bsr_count
    project_id = 'mba-pipeline'
    print(os.getcwd())

    # 2 hours time difference to real german time 14 -> 16 hour
    if not force_exec:
        on_time = datetime.time(14,00)
        off_time = datetime.time(9,00)
    else:
        on_time = datetime.time(14,00)
        off_time = datetime.time(13,59)
    
    replace("mba_crawler/settings.py", "CONCURRENT_REQUESTS = 5", "CONCURRENT_REQUESTS = 10")
    count = 0
    # is endless while loop if repeat is True/1
    while_condition = True
    while while_condition: 
        if marketplace_rotation:    
            marketplace = MARKETPLACES[count%len(MARKETPLACES)]  
        current_time = datetime.datetime.now().time()
        when, matching = check_time(current_time, on_time, off_time)
        #matching = True
        # execute function only between on_time and off_time
        if True: #matching or not repeat
            # sleep random to prevent com crawler and de crawler change their settings
            #time.sleep(random.randint(0,60))
            # create crawling data csv
            command = """sudo python3 create_url_csv.py {0} True --number_products={1} --proportion_priority_low_bsr_count={2}
            """.format(marketplace, number_products, proportion_priority_low_bsr_count)
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
            process.wait()

            # change settings
            # currently also us is crawled by all (also european) crawlers. Reason: only price is not shown in this case but BSR information can be crawled.
            command = """sudo python3 change_spider_settings.py de --use_public_proxies True
            """.format(marketplace)
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
            process.wait()
            
            # start crawling
            command = """sudo scrapy crawl mba_general_de -a marketplace={0} -a daily=True
            """.format(marketplace, number_products, proportion_priority_low_bsr_count)
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
            process.wait()

            if general_crawling_after_n_iter != 0 and (count%general_crawling_after_n_iter == 0):
                # start general crawling after every n iteration loops
                command = "sh crawl_general_product_page.sh"
                process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
                process.wait()
            
            if not repeat:
                while_condition = False
            count = count + 1
        else:
            print("Sleep for half an hour")
            time.sleep(30*60)

if __name__ == '__main__':
    main(sys.argv)
