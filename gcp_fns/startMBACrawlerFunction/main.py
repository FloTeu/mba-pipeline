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
    return """for cmd in "sudo python3 change_spider_settings.py de" "%s" "sudo python3 change_spider_settings.py com" "%s"; do
        eval ${cmd} &
        sleep %s
    done
    """ % (start_crawl_de_cmd, start_crawl_com_cmd, sleep_between_cmds)

def sequential_crawling_cmd(start_crawl_de_cmd, start_crawl_com_cmd):
    return """sudo python3 change_spider_settings.py de
%s
sudo python3 change_spider_settings.py com
%s
    """ % (start_crawl_de_cmd, start_crawl_com_cmd)

def get_overview_crawling_cmd(marketplace, pages, start_page=1, pod_product="shirt", sort="best_seller"):
    return "sudo scrapy crawl mba_overview -a marketplace={0} -a pod_product={3} -a sort={4} -a pages={1} -a start_page={2}".format(marketplace, pages, start_page, pod_product, sort)

def get_startup_script(is_daily_script, region_space, instance_name, number_products_total_additional=700, number_products_total=0):

    get_new_shirts = ""
    number_of_instances = 8
    #overview_crawl = parallel_crawling_cmd(get_overview_crawling_cmd("de", 100), get_overview_crawling_cmd("com", 50)) + "\n" + parallel_crawling_cmd(get_overview_crawling_cmd("de", 100, start_page=300), get_overview_crawling_cmd("com", 50, start_page=300)) + "\n" + parallel_crawling_cmd(get_overview_crawling_cmd("de", 10, sort="newest"), get_overview_crawling_cmd("com", 10, sort="newest"))
    overview_crawl = sequential_crawling_cmd(get_overview_crawling_cmd("de", 100), get_overview_crawling_cmd("com", 50)) + "\n" + sequential_crawling_cmd(get_overview_crawling_cmd("de", 100, start_page=300), get_overview_crawling_cmd("com", 50, start_page=300)) + "\n" + sequential_crawling_cmd(get_overview_crawling_cmd("de", 10, sort="newest"), get_overview_crawling_cmd("com", 10, sort="newest"))
    #overview_crawl = get_overview_crawling_cmd("de", 100) + "\n" + get_overview_crawling_cmd("de", 100, start_page=300) + "\n" + get_overview_crawling_cmd("de", 10, sort="newest")
    create_urls_for_general_crawl = ""
    stop_instance_by_itself = "--instance_name={} --stop_instance_by_itself=1".format(instance_name)
    # case sunday where we want to crawl all best sellers and newest
    if not is_daily_script:
        number_of_instances = 6
        get_new_shirts = '/usr/bin/python3 wc_mba.py "" PlhAyiU_2cQukrs_BZTuiQ de shirt newest'
        #overview_crawl = "/usr/bin/python3 wc_mba_detail_daily_preemptible_orga.py de True 3 --number_running_instances=4 --number_products 50 --number_products_total={} --time_break_sec 100 --seconds_between_crawl=40 --telegram_api_key 1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0 --telegram_chatid 869595848 --instance_name={} --stop_instance_by_itself=1".format(number_products_total_additional, instance_name)
        #overview_crawl = parallel_crawling_cmd(get_overview_crawling_cmd("de", 0), get_overview_crawling_cmd("com", 200, start_page=200)) + "\n" + parallel_crawling_cmd(get_overview_crawling_cmd("de", 0, sort="newest"), get_overview_crawling_cmd("com", 100, start_page=200, sort="newest"))
        overview_crawl = sequential_crawling_cmd(get_overview_crawling_cmd("de", 0), get_overview_crawling_cmd("com", 200, start_page=200)) + "\n" + sequential_crawling_cmd(get_overview_crawling_cmd("de", 0, sort="newest"), get_overview_crawling_cmd("com", 100, start_page=200, sort="newest"))
        create_urls_for_general_crawl = "sudo /usr/bin/python3 create_url_csv.py de False --number_products=-1"
        stop_instance_by_itself = ""

    general_start_crawler_script = parallel_crawling_cmd("sudo scrapy crawl mba_general_de -a marketplace=de -a daily=False", "sudo scrapy crawl mba_general_de -a marketplace=com -a daily=False")
    startup_script = '''#!/bin/sh
cd home/
rm -rf mba-pipeline/
git clone https://github.com/Flo95x/mba-pipeline.git
sudo pip3 install -r /home/mba-pipeline/crawler/mba/requirements.txt 
cd mba-pipeline
sudo pip3 install -e .
sudo pip3 install google-cloud-logging
yes | sudo apt-get install python-setuptools
sudo python3 setup.py build
sudo python3 setup.py install
sudo pip3 install -e .
cd crawler/mba/
#sudo git pull
sudo mkdir data
sudo mkdir data/shirts
sudo chmod 777 data/
sudo chmod 777 data/shirts
cd mba_crawler 
sudo mkdir proxy
cd proxy
sudo cp /home/flo_t_1995/proxies.json .
sudo cp /home/flo_t_1995/proxy_handler.py .
sudo cp /home/flo_t_1995/utils.py .
cd ..
{5}
wait
#sudo /usr/bin/python3 create_url_csv.py de False --number_products=700 & sudo /usr/bin/python3 create_url_csv.py com False --number_products=500
#wait
cd ..
#/usr/bin/python3 wc_mba_images.py de --number_chunks 0
yes Y | gcloud compute instances stop crawler-mba-auto-daily --zone us-west1-b
    '''.format(get_new_shirts, is_daily_script, region_space, number_of_instances, stop_instance_by_itself, overview_crawl, number_products_total, instance_name, create_urls_for_general_crawl, general_start_crawler_script)

    return startup_script


def instance_run(event, context):
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
    startup_script = get_startup_script(daily_script, region_space, instance,
                                        number_products_total=number_products_total)

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

from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingMBARequest, CrawlingMBAOverviewRequest, StartMBACrawlerFunctionRequest
from mwfunctions.pydantic.security_classes import MWSecuritySettings, EndpointId, EndpointServiceDevOp
from mwfunctions.environment import get_gcp_project
import requests
import pathlib
import ast
from typing import List, Union, Optional
from contextlib import suppress
import json

def start_crawler(event, context):
    from pprint import pprint
    from api_key import API_KEY

    if 'data' in event:
        data_dict_str = base64.b64decode(event['data']).decode('utf-8')
        data_dict = json.loads(data_dict_str)
        # data_dict = ast.literal_eval(data_dict_str)
    else:
        data_dict = {}

    print(data_dict)

    security_settings = MWSecuritySettings(file_path=f"{pathlib.Path(__file__).parent.resolve()}/security.json")

    is_dev = True
    endpoint_devop = EndpointServiceDevOp.DEV
    if get_gcp_project() in ["mba-pipeline","merchwatch"]:
        is_dev = False
        endpoint_devop = EndpointServiceDevOp.PROD

    crawler_start_request_list: List[Union[CrawlingMBAOverviewRequest, CrawlingMBAProductRequest]] = StartMBACrawlerFunctionRequest.parse_obj(data_dict).crawler_start_request_list
    split_after_n: int = StartMBACrawlerFunctionRequest.parse_obj(data_dict).split_after_n
    wait_n_minutes: Optional[int] = StartMBACrawlerFunctionRequest.parse_obj(data_dict).wait_n_minutes

    for crawler_start_request in crawler_start_request_list:
        url_parameter_append = f"&split_after_n={split_after_n}"
        crawler_start_request.reset_crawling_job_id()
        endpoint = security_settings.endpoints[EndpointId.CRAWLER_MW_API_OVERVIEW].devop2url[endpoint_devop] if isinstance(crawler_start_request, CrawlingMBAOverviewRequest) else None
        endpoint = security_settings.endpoints[EndpointId.CRAWLER_MW_API_PRODUCT].devop2url[endpoint_devop] if isinstance(crawler_start_request, CrawlingMBAProductRequest) else endpoint
        if isinstance(crawler_start_request, CrawlingMBAProductRequest):
            url_parameter_append = url_parameter_append + f"&wait_n_minutes={wait_n_minutes}"
        with suppress(requests.exceptions.ReadTimeout):
            r = requests.post(f"{endpoint}?access_token={API_KEY}{url_parameter_append}", crawler_start_request.json(), timeout=6)

    #
    #
    # start_page = int(data_dict["start_page"]) if "start_page" in data_dict else 1
    # pages = int(data_dict["pages"]) if "pages" in data_dict else 50
    # pod_product = data_dict["pod_product"] if "pod_product" in data_dict else "shirt"
    #
    # print(start_page, pages, pod_product, is_daily_script())
    #
    # crawling_mba_overview_request_list = []
    # # cloud run crawling can only handle 50 overview pages under 1 hour
    # # TODO: put image pipeline to another service like cloud functions to store images in storage (reduces max memory of cloud run + faster execution of overview request)
    # # monday to saturday
    # crawling_mba_overview_request_list.append(CrawlingMBAOverviewRequest(marketplace="com", debug=False, sort="best_seller", pod_product=pod_product,
    #                            pages=pages, start_page=start_page))
    # crawling_mba_overview_request_list.append(CrawlingMBAOverviewRequest(marketplace="de", debug=False, sort="best_seller", pod_product=pod_product,
    #                            pages=pages, start_page=start_page))
    # if is_daily_script():
    #     # only first 10 pages should be crawled for "newest" page on daily basis
    #     if start_page == 1:
    #         crawling_mba_overview_request_list.append(CrawlingMBAOverviewRequest(marketplace="com", debug=False, sort="newest", pod_product=pod_product,
    #                                    pages=10, start_page=start_page))
    #         crawling_mba_overview_request_list.append(CrawlingMBAOverviewRequest(marketplace="de", debug=False, sort="newest", pod_product=pod_product,
    #                                    pages=10, start_page=start_page))
    # # on sunday newsest page should be crawled like best_seller page
    # else:
    #     crawling_mba_overview_request_list.append(CrawlingMBAOverviewRequest(marketplace="com", debug=False, sort="newest", pod_product=pod_product,
    #                                pages=pages, start_page=start_page))
    #     crawling_mba_overview_request_list.append(CrawlingMBAOverviewRequest(marketplace="de", debug=False, sort="newest", pod_product=pod_product,
    #                                pages=pages, start_page=start_page))
    #
    #
    #     # Testing
    #     # crawling_mba_overview_request_list = [
    #     #     CrawlingMBAOverviewRequest(marketplace="de", debug=False, sort="best_seller", pod_product="shirt",
    #     #                                pages=10, start_page=1),
    #     #     CrawlingMBAOverviewRequest(marketplace="com", debug=False, sort="best_seller", pod_product="shirt",
    #     #                                pages=11, start_page=200),
    #     #     CrawlingMBAOverviewRequest(marketplace="de", debug=False, sort="newest", pod_product="shirt", pages=12,
    #     #                                start_page=1),
    #     #     CrawlingMBAOverviewRequest(marketplace="com", debug=False, sort="newest", pod_product="shirt", pages=13,
    #     #                                start_page=200)]
    #
    # if get_gcp_project() in ["mba-pipeline","merchwatch"]:
    #     endpoint_url = "https://mw-crawler-api-mhttow5wga-ey.a.run.app"
    # else:
    #     endpoint_url = "https://mw-crawler-api-ruzytvhzvq-ey.a.run.app"
    #
    # #instance_run(event, context)
    # for crawling_mba_overview_request in crawling_mba_overview_request_list:
    #     crawling_mba_overview_request.reset_crawling_job_id()
    #     try:
    #         r = requests.post(f"{endpoint_url}/start_mba_overview_crawler?wait_until_finished=true&access_token={API_KEY}", crawling_mba_overview_request.json(), timeout=6)
    #     except:
    #         pass
