import subprocess

def create_startup_script(marketplace, number_products, connection_timeout, time_break_sec, seconds_between_crawl, preemptible_code):
    startup_script = '''#!/bin/sh
cd home/
git clone https://github.com/Flo95x/mba-pipeline.git
pip3 install -r /home/mba-pipeline/crawler/mba/requirements.txt 
cd mba-pipeline/crawler/mba/
sudo chmod 777 data/mba_detail_page.html
/usr/bin/python3 /home/mba-pipeline/crawler/mba/wc_mba_detail.py {} --number_products {} --connection_timeout {} --time_break_sec {} --seconds_between_crawl {} --preemptible_code {}
    '''.format(marketplace, number_products, connection_timeout, time_break_sec, seconds_between_crawl, preemptible_code)
    # save product detail page locally
    with open("/home/f_teutsch/mba-pipeline/crawler/mba/pre_startup_script.sh", "w+") as f:
        f.write(startup_script)

def get_bash_create_pre_instance(instance_name, zone):
    bash_command = 'gcloud compute instances create {} --preemptible --zone {} --service-account mba-admin@mba-pipeline.iam.gserviceaccount.com --image-project mba-pipeline --image wc-mba-de-image --metadata-from-file startup-script=/home/f_teutsch/mba-pipeline/crawler/mba/pre_startup_script.sh --scopes storage-full,cloud-platform,bigquery'.format(instance_name, zone)
    return bash_command

def get_bash_describe_pre_instance(instance_name, zone):
    bash_command = 'gcloud compute instances describe {} --zone {}'.format(instance_name, zone)
    return bash_command


marketplace = "de"
if marketplace == "de":
    zone = "europe-west3-c"
# TODO implement other cases
else:
    print("Marketplace is not fully implemented")
pre_count = 1
create_startup_script(marketplace, 10, 5.0, 120, 20, "thread-" + str(pre_count))
pre_instance_name = "mba-"+marketplace+"-detail-pre-"+ str(pre_count)
bashCommand = get_bash_create_pre_instance(pre_instance_name,"europe-west3-c")
process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
bashCommand = get_bash_describe_pre_instance(pre_instance_name,"europe-west3-c")
process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
test = 0
