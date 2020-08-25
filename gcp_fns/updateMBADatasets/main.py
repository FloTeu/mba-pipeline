
import base64
import json
import logging
import time
from google.cloud import bigquery
import pandas as pd
from sklearn import preprocessing
import os 
from os.path import join
from datetime import date
import datetime 
import time
from plotly.offline import plot
import plotly.graph_objs as go
from plotly.graph_objs import Scatter 
from plotly.graph_objs import Layout 
import gc
from data_handler import DataHandler
import requests
import googleapiclient.discovery




def generate_startup(marketplace, instance_name, zone, chunk_size):
    # todo: delete instance at the end of the startup script
    startup_script = '''
#!/bin/sh
cd home/
rm -rf mba-pipeline/
git clone https://github.com/Flo95x/mba-pipeline.git
cd mba-pipeline/gcp_fns/updateMBADatasets
pip3 install -r requirements.txt 
/usr/bin/python3 execute_update.py --marketplace={0} --chunk_size={3}
echo deleting instance
yes Y | gcloud compute instances delete {1} --zone={2}
    '''.format(marketplace, instance_name, zone,chunk_size)
    return startup_script


def create_instance(marketplace, instance_name, zone, chunk_size):  

    compute = googleapiclient.discovery.build('compute', 'v1')
    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    startup_script = generate_startup(marketplace, instance_name, zone, chunk_size)

    config = {
        'name': instance_name,  # client_guid[:5],
        'machineType': machine_type,


        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',  # 602588395383-compute@developer.gserviceaccount.com
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/compute',
                # 'https://www.googleapis.com/auth/compute.readonly',

            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }]
        }
    }

    print('Creating Instance...')
    return compute.instances().insert(
        project="mba-pipeline",
        zone=zone,
        body=config).execute()


def updateBqShirtTables(event, context):
    
    PROJECT_ID = 'mba-pipeline'

    # [START parse_message]

    marketplace = "de"

    create_instance(marketplace, "cron-daily", "us-west1-b", 500)
    