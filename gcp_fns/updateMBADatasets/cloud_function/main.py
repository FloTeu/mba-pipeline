
import base64
import json
import logging
import time
import os 
from os.path import join
from datetime import date
import datetime 
import time
import gc
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
/usr/bin/python3 execute_update.py --marketplace=de --chunk_size={3} 
/usr/bin/python3 execute_update.py --marketplace=com --chunk_size={3}
wait
yes Y | gcloud compute instances stop {1} --zone {2}
    '''.format(marketplace, instance_name, zone,chunk_size)
    return startup_script

def generate_startup_sql_update(instance_name, instance_zone):
    startup_script = '''#!/bin/sh
cd home/
cd merchwatch/merchwatch
sleep 50m
./cloud_sql_proxy -instances="mba-pipeline:europe-west3:merchwatch-sql"=tcp:3306 &
sudo python3 manage.py runserver &
sleep 1m
sudo wget "127.0.0.1:8000/cron/daily"
sleep 45m
yes Y | gcloud compute instances stop {0} --zone {1}'''.format(instance_name, instance_zone)

    return startup_script

def create_instance(marketplace, instance_name, zone, chunk_size):  

    compute = googleapiclient.discovery.build('compute', 'v1')
    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    startup_script = generate_startup(marketplace, instance_name, zone, chunk_size)

    image_response = compute.images().get(image='cron-image', project='mba-pipeline').execute()
    source_disk_image = image_response['selfLink']


    config = {
        'name': instance_name,  # client_guid[:5],
        'machineType': machine_type,

        # Specify the boot disk and the image to use    as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],


        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],


        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',  # 602588395383-compute@developer.gserviceaccount.com
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/datastore',
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

def start_cron_daily(marketplace, chunk_size=500):
    from pprint import pprint
    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials

    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)

    # Project ID for this request.
    project = 'mba-pipeline' 

    # The name of the zone for this request.
    zone = 'europe-west3-c' 

    # Name of the instance resource to start.
    instance = 'cron-daily-de' 

    startup_script = generate_startup(marketplace, instance, zone, chunk_size)

    request = service.instances().get(project=project, zone=zone, instance=instance)
    response = request.execute()
    fingerprint = response["metadata"]["fingerprint"]
    
    metadata_body = {
        "fingerprint": fingerprint,
  "items": [
    {
      "key": "startup-script",
      "value": startup_script
    }
  ]
    }

    request = service.instances().setMetadata(project=project, zone=zone, instance=instance, body=metadata_body)
    response = request.execute()
    request = service.instances().start(project=project, zone=zone, instance=instance)
    response = request.execute()


def updateBqShirtTables(event, context):
    
    PROJECT_ID = 'mba-pipeline'

    # [START parse_message]

    # does not matter at the moment
    marketplace = "de"

    #create_instance(marketplace, "cron-daily", "us-west1-b", 500)
    start_cron_daily(marketplace, chunk_size=10000)
test = 0
