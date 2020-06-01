import subprocess
import os
import argparse
import time 
import sys
import numpy as np 
import pandas as pd
from google.cloud import bigquery
import datetime

def create_startup_script(marketplace, number_products, connection_timeout, time_break_sec, seconds_between_crawl, preemptible_code, pre_instance_name):
    startup_script = '''#!/bin/sh
cd home/
git clone https://github.com/Flo95x/mba-pipeline.git
pip3 install -r /home/mba-pipeline/crawler/mba/requirements.txt 
cd mba-pipeline/crawler/mba/
sudo chmod 777 data/mba_detail_page.html
/usr/bin/python3 /home/mba-pipeline/crawler/mba/wc_mba_detail.py {} --number_products {} --connection_timeout {} --time_break_sec {} --seconds_between_crawl {} --preemptible_code {} --pre_instance_name {}
    '''.format(marketplace, number_products, connection_timeout, time_break_sec, seconds_between_crawl, preemptible_code, pre_instance_name)
    # save product detail page locally
    with open("/home/f_teutsch/mba-pipeline/crawler/mba/pre_startup_script.sh", "w+") as f:
        f.write(startup_script)

def get_bash_create_pre_instance(instance_name, zone):
    bash_command = 'gcloud compute instances create {} --preemptible --zone {} --service-account mba-admin@mba-pipeline.iam.gserviceaccount.com --image-project mba-pipeline --image wc-mba-de-image --metadata-from-file startup-script=/home/f_teutsch/mba-pipeline/crawler/mba/pre_startup_script.sh --scopes storage-full,cloud-platform,bigquery'.format(instance_name, zone)
    return bash_command

def get_bash_start_pre_instance(instance_name, zone):
    bash_command = 'gcloud compute instances start {} --zone {}'.format(instance_name, zone)
    return bash_command

def get_bash_describe_pre_instance(instance_name, zone):
    bash_command = 'gcloud compute instances describe {} --zone {}'.format(instance_name, zone)
    return bash_command

def get_bash_delete_pre_instance(instance_name, zone):
    bash_command = 'yes Y | gcloud compute instances delete {} --zone {}'.format(instance_name, zone)
    return bash_command

def get_currently_running_instance(number_running_instances, marketplace, zone):
    currently_running_instance = []
    for i in range(number_running_instances):
        pre_instance_name = "mba-"+marketplace+"-detail-pre-"+ str(i+1)
        bashCommand = get_bash_describe_pre_instance(pre_instance_name,zone)
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        try:
            status = str(output).split("status: ")[1].split("\\")[0]
        except:
            status = "NOT EXISTEND"
        if status.upper() == "RUNNING":
            currently_running_instance.append(pre_instance_name)

    return currently_running_instance

def get_currently_terminated_instance(number_running_instances, marketplace, zone):
    currently_terminated_instance = []
    for i in range(number_running_instances):
        pre_instance_name = "mba-"+marketplace+"-detail-pre-"+ str(i+1)
        bashCommand = get_bash_describe_pre_instance(pre_instance_name,zone)
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        try:
            status = str(output).split("status: ")[1].split("\\")[0]
        except:
            status = "NOT EXISTEND"
        if status.upper() == "TERMINATED":
            currently_terminated_instance.append(pre_instance_name)

    return currently_terminated_instance

def update_preemptible_logs(pree_id, marketplace, status):
    project_id = 'mba-pipeline'
    timestamp = datetime.datetime.now()
    dataset_id = "preemptible_logs"
    table_id = "mba_detail_" + marketplace + "_preemptible_%s_%s_%s"%(timestamp.year, timestamp.month, timestamp.day)
    reservation_table_id = dataset_id + "." + table_id
    bq_client = bigquery.Client(project=project_id)
    # get reservation logs
    df_reservation = bq_client.query("SELECT * FROM " + reservation_table_id + " t0 WHERE t0.pree_id = '{}' order by t0.timestamp DESC".format(pree_id)).to_dataframe().drop_duplicates()
    df_reservation_status = df_reservation.drop_duplicates("asin")
    # get list of asins that are currently blocked by preemptible instances
    df_reservation_status_blocked = df_reservation_status[df_reservation_status["status"] == "blocked"]
    print("%s asins where not correctly crawled by %s" %(len(df_reservation_status_blocked), pree_id))
    df_reservation_status_blocked['timestamp'] = timestamp
    df_reservation_status_blocked['timestamp'] = df_reservation_status_blocked['timestamp'].astype('datetime64')
    df_reservation_status_blocked['status'] = status
    df_reservation_status_blocked.to_gbq("preemptible_logs.mba_detail_" + marketplace + "_preemptible_%s_%s_%s"%(timestamp.year, timestamp.month, timestamp.day),project_id="mba-pipeline", if_exists="append")

def start_instance(marketplace, number_running_instances, number_products,connection_timeout, time_break_sec, seconds_between_crawl, pree_id, id, zone):
    pre_instance_name = "mba-"+marketplace+"-detail-pre-"+ str(id)
    # get terminated instances
    currently_terminated_instance = get_currently_terminated_instance(number_running_instances, marketplace, zone)
    # if instance is terminated it should be restarted and not recreated
    if pre_instance_name in currently_terminated_instance:
        bashCommand = get_bash_start_pre_instance(pre_instance_name,zone)
    # if instance does not exists it should be created
    else:
        create_startup_script(marketplace, number_products, connection_timeout, time_break_sec, seconds_between_crawl, pree_id, pre_instance_name)
        bashCommand = get_bash_create_pre_instance(pre_instance_name,zone)
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--number_running_instances', default=3, type=int, help='Number of preemptible instances that shoul run parallel. Default is 3.')
    parser.add_argument('--number_products', default=10, type=int, help='Number of products/shirts that shoul be crawled. If 0, every image that is not already crawled will be crawled.')
    parser.add_argument('--connection_timeout', default=10.0, type=float, help='Time that the request operation has until its breaks up. Default: 10.0 sec')
    parser.add_argument('--time_break_sec', default=240, type=int, help='Time in seconds the script tries to get response of certain product. Default 240 sec')
    parser.add_argument('--seconds_between_crawl', default=20, type=int, help='Time in seconds in which no proxy/ip shoul be used twice for crawling. Important to prevent being blacklisted. Default 20 sec')

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    number_running_instances = args.number_running_instances
    number_products = args.number_products
    connection_timeout = args.connection_timeout
    time_break_sec = args.time_break_sec
    seconds_between_crawl = args.seconds_between_crawl

    if marketplace == "de":
        zone = "europe-west3-c"
    # TODO implement other cases
    else:
        print("Marketplace is not fully implemented")

    while True:
        currently_running_instance = get_currently_running_instance(number_running_instances, marketplace, zone)
        currently_running_ids = [int(i.split("-")[-1]) for i in currently_running_instance]
        # if every instance is runnning program sleeps for 5 minutes
        if len(currently_running_instance) == number_running_instances:
            time.sleep(60 * 5)
        # else preemptible logs need to be updated in case of failure and new instance need to be started
        else:
            not_running_threat_ids = [x for x in np.arange(1,number_running_instances+1, 1).tolist() if x not in currently_running_ids]
            for id in not_running_threat_ids:
                pree_id = "thread-" + str(id)
                # update preemptible logs with failure statement
                update_preemptible_logs(pree_id, marketplace, "failure")
                # start instance and startupscript
                start_instance(marketplace, number_running_instances, number_products,connection_timeout, time_break_sec, seconds_between_crawl, pree_id, id, zone)
                # before next instance starts 10 seconds should the script wait
                time.sleep(10)

if __name__ == '__main__':
    main(sys.argv)
