import base64
from datetime import datetime
import subprocess

def get_current_day():
   return datetime.today().weekday() 

def get_region_space():
    weekday = get_current_day()
    # 0 = Monday, 6 = Sunday
    if weekday in [0, 3]:
        return 1
    elif weekday in [1, 4]:
        return 2
    elif weekday in [2, 5]:
        return 3
    else:
        return 1

def get_number_products_total():
    # 0 means crawl everything
    weekday = get_current_day()
    # 0 = Monday, 6 = Sunday
    if weekday in [1, 3, 5]:
        return 500
    elif weekday in [0, 2, 4]:
        return 500
    else:
        return 0

def is_daily_script():
    weekday = get_current_day()
    # 0 = Monday, 6 = Sunday
    if weekday in [0, 1, 2, 3, 4, 5]:
        return True
    else:
        return False

def parallel_crawling_cmd(start_crawl_de_cmd, start_crawl_com_cmd, sleep_between_cmds=5):
    return """for cmd in "python3 change_spider_settings.py de" "%s" "python3 change_spider_settings.py com" "%s"; do
        eval ${cmd} &
        sleep %s
    done
    """ % (start_crawl_de_cmd, start_crawl_com_cmd, sleep_between_cmds)

def get_overview_crawling_cmd(marketplace, pages, start_page=1, pod_product="shirt", sort="best_seller"):
    return "sudo scrapy crawl mba_overview -a marketplace={0} -a pod_product={3} -a sort={4} -a pages={1} -a start_page={2}".format(marketplace, pages, start_page, pod_product, sort)

def get_startup_script(is_daily_script, region_space, instance_name, number_products_total_additional=700, number_products_total=0):

    get_new_shirts = ""
    number_of_instances = 8
    overview_crawl = parallel_crawling_cmd(get_overview_crawling_cmd("de", 100), get_overview_crawling_cmd("com", 100)) + "\n" + parallel_crawling_cmd(get_overview_crawling_cmd("de", 100, start_page=300), get_overview_crawling_cmd("com", 100, start_page=300)) + "\n" + parallel_crawling_cmd(get_overview_crawling_cmd("de", 10, sort="newest"), get_overview_crawling_cmd("com", 10, sort="newest"))
    create_urls_for_general_crawl = ""
    stop_instance_by_itself = "--instance_name={} --stop_instance_by_itself=1".format(instance_name)
    # case sunday where we want to crawl all best sellers and newest
    if not is_daily_script:
        number_of_instances = 6
        get_new_shirts = '/usr/bin/python3 wc_mba.py "" PlhAyiU_2cQukrs_BZTuiQ de shirt newest'
        #overview_crawl = "/usr/bin/python3 wc_mba_detail_daily_preemptible_orga.py de True 3 --number_running_instances=4 --number_products 50 --number_products_total={} --time_break_sec 100 --seconds_between_crawl=40 --telegram_api_key 1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0 --telegram_chatid 869595848 --instance_name={} --stop_instance_by_itself=1".format(number_products_total_additional, instance_name)
        overview_crawl = parallel_crawling_cmd(get_overview_crawling_cmd("de", 0), get_overview_crawling_cmd("com", 0)) + "\n" + parallel_crawling_cmd(get_overview_crawling_cmd("de", 0, sort="newest"), get_overview_crawling_cmd("com", 0, sort="newest"))
        create_urls_for_general_crawl = "sudo /usr/bin/python3 create_url_csv.py de False --number_products=-1"
        stop_instance_by_itself = ""

    general_start_crawler_script = parallel_crawling_cmd("sudo scrapy crawl mba_general_de -a marketplace=de -a daily=False", "sudo scrapy crawl mba_general_de -a marketplace=com -a daily=False")
    startup_script = '''#!/bin/sh
cd home/
rm -rf mba-pipeline/
git clone https://github.com/Flo95x/mba-pipeline.git
sudo pip3 install -r /home/mba-pipeline/crawler/mba/requirements.txt 
cd mba-pipeline/crawler/mba/
#sudo git pull
sudo mkdir data
sudo mkdir data/shirts
sudo chmod 777 data/
sudo chmod 777 data/shirts
/usr/bin/python3 wc_mba_images.py de --number_chunks 0
cd mba_crawler 
sudo mkdir proxy
cd proxy
sudo cp /home/flo_t_1995/proxies.json .
sudo cp /home/flo_t_1995/proxy_handler.py .
sudo cp /home/flo_t_1995/utils.py .
cd ..
{5}
sudo /usr/bin/python3 create_url_csv.py de False --number_products=700 & sudo /usr/bin/python3 create_url_csv.py com False --number_products=700
{9}
# daily crawler with public proxies handles this task
# sudo /usr/bin/python3 create_url_csv.py de True --number_products={6}  --proportion_priority_low_bsr_count=0.9
# sudo scrapy crawl mba_general_de -a daiy=True
cd ..
/usr/bin/python3 wc_mba_images.py de --number_chunks 0
yes Y | gcloud compute instances stop crawler-mba-auto-daily --zone us-west1-b
    '''.format(get_new_shirts, is_daily_script, region_space, number_of_instances, stop_instance_by_itself, overview_crawl, number_products_total, instance_name, create_urls_for_general_crawl, general_start_crawler_script)

    return startup_script


def start_crawler(event, context):
    from pprint import pprint

    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials

    credentials = GoogleCredentials.get_application_default()

    service = discovery.build('compute', 'v1', credentials=credentials)

    # Project ID for this request.
    project = 'mba-pipeline' 

    # The name of the zone for this request.
    zone = 'us-west1-b' 

    # Name of the instance resource to start.
    instance = 'crawler-mba-auto-daily' 

    region_space = get_region_space()
    daily_script = is_daily_script()
    number_products_total = get_number_products_total()
    startup_script = get_startup_script(daily_script, region_space, instance, number_products_total=number_products_total)

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

