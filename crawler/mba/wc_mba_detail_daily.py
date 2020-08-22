from bs4 import BeautifulSoup
import requests 
import pandas as pd
import numpy as np
import argparse
import sys
import urllib.request
from urllib.request import Request, urlopen
import urllib.parse as urlparse
from urllib.parse import quote_plus
from urllib.parse import unquote_plus
from urllib.parse import urlencode
from urllib.parse import urljoin
import mba_url_creator as url_creator
import utils
import random 
from lxml.html import fromstring
from itertools import cycle
import datetime 
import shutil 
from google.cloud import storage
from google.cloud import bigquery
import os
import time 
from proxy_requests import ProxyRequests
import dateparser
import uuid

def get_asin_product_detail_daily_crawled(marketplace):
    project_id = 'mba-pipeline'
    reservationdate = datetime.datetime.now()
    dataset_id = "preemptible_logs"
    table_id = "mba_detail_daily_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day)
    reservation_table_id = dataset_id + "." + table_id
    bq_client = bigquery.Client(project=project_id)
    # TODO get those which are not already crawled today
    # TODO remove asins that return a 404 (not found) error
    SQL_STATEMENT = '''
    SELECT t0.asin, t0.url_product, t2.bsr_count FROM mba_{0}.products t0 LEFT JOIN (SELECT * FROM mba_{0}.products_details_daily WHERE DATE(timestamp) = '{1}-{2}-{3}' or price_str = '404') t1 on t0.asin = t1.asin 
        LEFT JOIN 
        (
        SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
                    AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min, COUNT(*) as bsr_count,
                    AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
                    FROM `mba-pipeline.mba_{0}.products_details_daily`
            where bsr != 404
            group by asin
        ) t2 on t0.asin = t2.asin 
        where t1.asin IS NULL 
        order by t2.bsr_count
    '''.format(marketplace, reservationdate.year, reservationdate.month, reservationdate.day)
    #df_product_details = bq_client.query("SELECT t0.asin, t0.url_product FROM mba_" + marketplace + ".products t0 LEFT JOIN (SELECT * FROM mba_" + marketplace + ".products_details_daily WHERE DATE(timestamp) = '%s-%s-%s' or price_str = '404') t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp" %(reservationdate.year, reservationdate.month, reservationdate.day)).to_dataframe().drop_duplicates()
    df_product_details = bq_client.query(SQL_STATEMENT).to_dataframe().drop_duplicates()

    if utils.does_table_exist(project_id, dataset_id, table_id):
        # get reservation logs
        df_reservation = bq_client.query("SELECT * FROM " + reservation_table_id + " t0 order by t0.timestamp DESC").to_dataframe().drop_duplicates()
        df_reservation_status = df_reservation.drop_duplicates("asin")
        # get list of asins that are currently blocked by preemptible instances
        asins_blocked = df_reservation_status[df_reservation_status["status"] == "blocked"]["asin"].tolist()
        # filter asins for those which are not blocked
        matching_asins = df_product_details["asin"].isin(asins_blocked)
        print("%s asins are currently blocked and will not be crawled" % str(len([i for i in matching_asins if i == True])))
        df_product_details = df_product_details[~matching_asins]
    
    return df_product_details

def get_product_information_de(list_product_information):
    customer_recession_score_mean = [0.0]
    customer_recession_score = [""]
    customer_recession_count = [0]
    mba_bsr_str = [""]
    mba_bsr = [0]
    array_mba_bsr = []
    array_mba_bsr_categorie = []

    for info in list_product_information:
        info_text = info.get_text().lower()
        if "rezension" in info_text or "review" in info_text:
            try:
                customer_recession_score = [info.find("span", class_="a-declarative").find("a").find("i").get_text()]
                customer_recession_count = [int(info.find("span", class_="a-size-small").find("a").get_text().split(" ")[0])]
            except:
                customer_recession_score = [""]
                customer_recession_count = [0]
            try:
                customer_recession_score_mean = [float(customer_recession_score[0].split(" von")[0].replace(",","."))]
            except:
                customer_recession_score_mean = [0.0]
        try:
            if info["id"] == "SalesRank":
                mba_bsr_str = [info.get_text().replace("\n", "")]
                bsr_iterator = mba_bsr_str[0].split("Nr. ")
                bsr_iterator = bsr_iterator[1:len(bsr_iterator)]
                for bsr_str in bsr_iterator:
                    bsr = int(bsr_str.split("in")[0].replace(".", ""))
                    array_mba_bsr.append(bsr)
                    bsr_categorie = bsr_str.split("(")[0].split("in")[1].replace("\xa0", "").strip()
                    array_mba_bsr_categorie.append(bsr_categorie)
                mba_bsr = [int(bsr_iterator[0].split("in")[0].replace(".", ""))]
        except:
            pass

    return customer_recession_score_mean, customer_recession_score, customer_recession_count, mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie

def get_product_information(marketplace, list_product_information):
    if marketplace == "de":
        return get_product_information_de(list_product_information)
    else:
        assert False, "Marketplace not known"


def get_product_detail_daily_df(soup, asin, url_mba, marketplace, api_key="", chat_id=""):
    product_information = soup.find("div", id="detail-bullets_feature_div")
    if product_information == None:
        product_information = soup.find("div", id="dpx-detail-bullets_feature_div")
    if product_information == None:
        utils.send_msg(chat_id, "Could not find detail-bullets_feature_div in soup for asin: " + str(asin), api_key)
        raise ValueError

    # get all headline infos 
    try:
        price_str = [soup.find("span", id="priceblock_ourprice").get_text()]
    except:
        utils.send_msg(chat_id, "Could not get price of product: " + str(asin), api_key)
        price_str = ["ERROR"]
    try:
        price = float(price_str[0].split("\xa0")[0].replace(",","."))
    except:
        price = 0.0
    # get all product information
    list_product_information = product_information.find("ul").find_all("li")
    try:
        customer_recession_score_mean, customer_recession_score, customer_recession_count, mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie = get_product_information(marketplace, list_product_information)
    except:
        utils.send_msg(chat_id, "Could not get get_product_information of product: " + str(asin), api_key)
        raise ValueError

    crawlingdate = [datetime.datetime.now()]

    product_information_str = str(product_information) + ",PRICE:" + price_str[0]
    df_products_details = pd.DataFrame(data={"asin":[asin],"price":price,"price_str":price_str,"bsr":mba_bsr,"bsr_str":mba_bsr_str, "array_bsr": [array_mba_bsr], "array_bsr_categorie": [array_mba_bsr_categorie],"customer_review_score_mean":customer_recession_score_mean,"customer_review_score": customer_recession_score,"customer_review_count": customer_recession_count, "timestamp":crawlingdate, "product_information_html":product_information_str})
    # transform date/timestamo columns to datetime objects
    df_products_details['timestamp'] = df_products_details['timestamp'].astype('datetime64')

    return df_products_details

# global variable with proxie list and time since crawling starts 
list_country_proxies = ["de", "dk", "pl", "fr", "ua", "us", "cz"]
list_country_proxies = ["de", "dk", "pl"]
proxy_list, country_list = ["to_delete"], ["to_delete"]#utils.get_proxies_with_country(list_country_proxies, True)
last_successfull_crawler = proxy_list[0]
time_since_last_crawl = None
df_successfull_proxies = None
#df_successfull_proxies = pd.DataFrame({"proxy": ["213.213"], "country": ["DE"], "successCount":[1], "errorCount": [0], "errors":[[]]})

test = 0
def get_response(marketplace, url_product_asin, api_key, chat_id, use_proxy=True, connection_timeout=5.0,time_break_sec=60, seconds_between_crawl=20):
    time_start = time.time()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
    
    
    # set global variables
    global proxy_list
    global country_list
    global last_successfull_crawler
    global time_since_last_crawl
    global df_successfull_proxies

    if not use_proxy:
        last_successfull_crawler = "gcp_proxy"

    def reset_proxies_and_country_list():
        global proxy_list
        global country_list    
        global df_successfull_proxies

        del proxy_list[0]
        del country_list[0]
        # if no proxie is left start to get new ones
        if len(proxy_list) == 0:
            if type(df_successfull_proxies) != type(None):
                suc_proxy_list, suc_country_list = df_successfull_proxies["proxy"].tolist(), df_successfull_proxies["country"].tolist()
            else:
                suc_proxy_list, suc_country_list = [],[]
            new_proxy_list, new_country_list = utils.get_proxies_with_country(list_country_proxies, True)
            proxy_list = suc_proxy_list + new_proxy_list
            country_list = suc_country_list + new_country_list
    
    def set_error_data(proxy, country, message):
        global df_successfull_proxies
        if type(df_successfull_proxies) == type(None):
            df_successfull_proxies = pd.DataFrame(data={"proxy": [proxy], "country": [country], "successCount":[0],"errorCount": [0], "errors":[[]]})
        # add error if preveasly successfull proxy was used
        if proxy in df_successfull_proxies["proxy"].tolist():
            index = df_successfull_proxies[df_successfull_proxies["proxy"] == proxy].index.values[0]
            df_successfull_proxies.loc[index, "errorCount"]  = df_successfull_proxies.loc[index, "errorCount"] + 1
            df_successfull_proxies.loc[index, "errors"].append(message)
            
    while len(proxy_list) > 0:
        # if time break is reached the loop will be broken
        elapsed_time = time.time() - time_start
        if elapsed_time > time_break_sec:
            print("Time break condition was reached. Response is empty")
            break
        
        if use_proxy:
            reset_proxies_and_country_list()
            proxy = proxy_list[0]
            country = country_list[0]
        else:
            proxy = "gcp_proxy"
            country = "gcp_country"
        print("Proxy: %s is used | %s left" % (proxy, len(proxy_list)))
        proxies={"http": 'http://' + proxy, "https": 'https://' + proxy}
        
        # wait seconds to make sure successfull proxie is not blacklisted
        if last_successfull_crawler == proxy and time_since_last_crawl != None and seconds_between_crawl > (time.time()-time_since_last_crawl):
            # add randomnes of plus or minus 5 seconds
            wait_seconds = seconds_between_crawl - (time.time()-time_since_last_crawl) - random.randint(-5, 5)
            print("Same proxie used, wait for %.2f seconds" % wait_seconds)
            time.sleep(wait_seconds)

        try:
            headers = utils.get_random_headers(marketplace)
            # if no proxy server is used, the script should wait after each response try
            if not use_proxy:
                time_since_last_crawl = time.time()
            # try to get response
            if use_proxy:
                response = requests.get(url_product_asin, timeout=connection_timeout, proxies=proxies, headers=headers)#, verify=False)
            # use gcp ip instead of proxy
            else:
                response = requests.get(url_product_asin, timeout=connection_timeout, headers=headers)
            if response.status_code == 200:
                if "captcha" in response.text.lower():
                    print("No Match: Got code 200, but captcha is requested. User agent: %s. Try next proxy... (Country: %s)" % (headers['user-agent'],country))
                    set_error_data(proxy, country, "captcha")
                    if use_proxy:
                        reset_proxies_and_country_list()
                    continue
                # successfull crawl
                else:
                    # start global time variable to check duration between successfull crawling with same proxie
                    last_successfull_crawler = proxy
                    if use_proxy:
                        time_since_last_crawl = time.time()
                    # save successfull proxy in dataframe
                    if type(df_successfull_proxies) == type(None):
                        df_successfull_proxies = pd.DataFrame(data={"proxy": [proxy], "country": [country], "successCount":[1],"errorCount": [0], "errors":[[]]})
                    else:
                        # if proxy already exists, success count should increase
                        if len(df_successfull_proxies[df_successfull_proxies["proxy"] == proxy]) != 0:
                            index = df_successfull_proxies[df_successfull_proxies["proxy"] == proxy].index.values[0]
                            df_successfull_proxies.loc[index, "successCount"]  = df_successfull_proxies.loc[index, "successCount"] + 1
                        # else new proxy is added to dataframe
                        else:
                            df_successfull_proxies.append({"proxy": [proxy], "country": [country_list], "successCount":[1], "errorCount": [0], "errors":[[]]}, ignore_index=None)
                    
                    print("Match: Scrape successfull in %.2f minutes (Country: %s)" % ((time.time() - time_start)/60, country))
                    return response
            else:
                # Save that information
                print("No Match: Status code: " + str(response.status_code) + ", user agent: %s (Country: %s)" % (headers['user-agent'], country))
                if response.status_code == 404:
                    print("Url not found. Product was probably removed")
                    return 404
                utils.send_msg(chat_id, "Scrap not successfull. Got status code: {}".format(str(response.status_code)), api_key)
                set_error_data(proxy, country, "Status code: " + str(response.status_code))
                if use_proxy:
                    reset_proxies_and_country_list()
                continue
        except Exception as e:
            print("No Match: got exception: %s (Country: %s)" % (type(e).__name__, country))
            #print(str(e))
            set_error_data(proxy, country, "Exception: " + str((type(e).__name__)))
            if use_proxy:
                reset_proxies_and_country_list()
            continue

    # return None if no response could be crawled
    return None

def update_reservation_logs(marketplace, asin, status, preemptible_code, ip_address, bsr, price, pre_instance_name, zone, api_key, chat_id):
    global df_successfull_proxies
    error_str = ""
    if type(df_successfull_proxies) != type(None):
        error_str = "Error count: " + str(df_successfull_proxies.iloc[0]["errorCount"]) + " errors: " + ",".join(df_successfull_proxies.iloc[0]["errors"])

    reservationdate = datetime.datetime.now()
    df_reservation = pd.DataFrame({"asin": [str(asin)], "status": [str(status)], "pree_id": [str(preemptible_code)], "ip_address":[ip_address], "error_log": [error_str], "timestamp": [reservationdate], "bsr":[bsr], "price":[price]})
    df_reservation['timestamp'] = df_reservation['timestamp'].astype('datetime64')
    # todo fix the error of to many requests in bigquery 
    try:
        df_reservation.to_gbq("preemptible_logs.mba_detail_daily_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day),project_id="mba-pipeline", if_exists="append")
    except:
        stop_instance(pre_instance_name, zone, "Can not update bigquery reservation table", api_key, chat_id)

def update_reservation_logs_list(marketplace, asin_list, status, preemptible_code, ip_address, bsr_list, price_list, pre_instance_name, zone):
    global df_successfull_proxies
    error_str = ""
    if type(df_successfull_proxies) != type(None):
        error_str = "Error count: " + str(df_successfull_proxies.iloc[0]["errorCount"]) + " errors: " + ",".join(df_successfull_proxies.iloc[0]["errors"])

    reservationdate = datetime.datetime.now()
    df_reservation = pd.DataFrame({"asin": asin_list, "status": [str(status) for i in asin_list], "pree_id": [str(preemptible_code) for i in asin_list], "ip_address":[str(ip_address) for i in asin_list], "error_log": [str(error_str) for i in asin_list], "timestamp": [str(reservationdate) for i in asin_list], "bsr":[str(bsr) for bsr in bsr_list], "price":price_list})
    df_reservation['timestamp'] = df_reservation['timestamp'].astype('datetime64')
    # todo fix the error of to many requests in bigquery 
    try:
        df_reservation.to_gbq("preemptible_logs.mba_detail_daily_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day),project_id="mba-pipeline", if_exists="append")
    except:
        time.sleep(10)
        try:
            df_reservation.to_gbq("preemptible_logs.mba_detail_daily_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day),project_id="mba-pipeline", if_exists="append")
        except:
            pass
        #stop_instance(pre_instance_name, zone)

def stop_instance(pre_instance_name, zone, msg, api_key, chat_id):
    ip_adress = get_extrenal_ip(pre_instance_name, zone)
    utils.send_msg(chat_id, "Instance {} is stopped | IP: {} | Reason: {}".format(pre_instance_name, str(ip_adress), msg), api_key)
    bashCommand = "yes Y | gcloud compute instances stop {} --zone {}".format(pre_instance_name, zone)
    stream = os.popen(bashCommand)
    output = stream.read()

def get_extrenal_ip(pre_instance_name, zone):
    bashCommand = "yes Y | gcloud compute instances describe {} --zone {}  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'".format(pre_instance_name, zone)
    stream = os.popen(bashCommand)
    ip_address = stream.read()
    return ip_address.replace("\n", "")

def make_reservation(df_product_details_tocrawl,number_products,preemptible_code,ip_address,marketplace,pre_instance_name, zone, api_key, chat_id):
    reservationdate = datetime.datetime.now()
    df_reservation = df_product_details_tocrawl.iloc[0:number_products][["asin"]].copy()
    df_reservation['status'] = "blocked"
    df_reservation['pree_id'] = preemptible_code
    df_reservation['ip_address'] = ip_address
    df_reservation['error_log'] = ""
    df_reservation['timestamp'] = reservationdate
    df_reservation['bsr'] = ""
    df_reservation['price'] = ""
    df_reservation['timestamp'] = df_reservation['timestamp'].astype('datetime64')
    try:
        df_reservation.to_gbq("preemptible_logs.mba_detail_daily_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day),project_id="mba-pipeline", if_exists="append")
    except:
        stop_instance(pre_instance_name, zone, "Can not update big query reservation", api_key, chat_id)

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--telegram_api_key',default="", help='API key of mba bot', type=str)
    parser.add_argument('--telegram_chatid', default="", help='Id of channel like private chat or group channel', type=str)
    parser.add_argument('--number_products', default=10, type=int, help='Number of products/shirts that shoul be crawled. If 0, every image that is not already crawled will be crawled.')
    parser.add_argument('--connection_timeout', default=10.0, type=float, help='Time that the request operation has until its breaks up. Default: 10.0 sec')
    parser.add_argument('--time_break_sec', default=240, type=int, help='Time in seconds the script tries to get response of certain product. Default 240 sec')
    parser.add_argument('--seconds_between_crawl', default=20, type=int, help='Time in seconds in which no proxy/ip shoul be used twice for crawling. Important to prevent being blacklisted. Default 20 sec')
    parser.add_argument('--preemptible_code', default="0", type=str, help='Identifier of instance for pree logs. Default 0 which leads to GUID.')
    parser.add_argument('--pre_instance_name', default="", type=str, help='Name of instance. Important: if set, script will stop instance after successfull operation. Default "".')
    parser.add_argument('--zone', default="", type=str, help='Zone of instance. Must fit to close the instance correctly after successfull run. Default mayor zone of marketplace.')

    print(os.getcwd())
    print(argv)

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    api_key = args.telegram_api_key
    chat_id = args.telegram_chatid
    number_products = args.number_products
    connection_timeout = args.connection_timeout
    time_break_sec = args.time_break_sec
    seconds_between_crawl = args.seconds_between_crawl
    preemptible_code = args.preemptible_code
    pre_instance_name = args.pre_instance_name
    zone = args.zone
    if zone == "":
        zone = utils.get_zone_of_marketplace(marketplace)
    ip_address = get_extrenal_ip(pre_instance_name, zone)

    if preemptible_code == "0":
        preemptible_code = uuid.uuid4().hex

    # get all arguments
    args = parser.parse_args()

    # get asins which are not already crawled
    df_product_details_tocrawl_total = get_asin_product_detail_daily_crawled(marketplace)
    df_product_details_tocrawl = df_product_details_tocrawl_total[0:int(number_products/2)].reset_index(drop=True)
    # remove asins that are in priority order
    df_product_details_tocrawl_total = df_product_details_tocrawl_total[~df_product_details_tocrawl_total.asin.isin(df_product_details_tocrawl["asin"].tolist())]
    df_product_details_tocrawl = df_product_details_tocrawl.append(df_product_details_tocrawl_total.sample(frac=1).reset_index(drop=True))
    if len(df_product_details_tocrawl) == 0:
        print("no data to crawl")
        if pre_instance_name != "" and "pre" in pre_instance_name:
            stop_instance(pre_instance_name, zone, "No data to crawl", api_key, chat_id)
        return 0
    #df_product_details = pd.DataFrame(data={"asin": ["B07RVNJHZL"], "url_product": ["adwwadwad"]})
    df_product_details_tocrawl["url_product_asin"] =  df_product_details_tocrawl.apply(lambda x: "https://www.amazon."+marketplace+"/dp/"+x["asin"], axis=1)
    
    # if number_images is equal to 0, evry image should be crawled
    if number_products == 0:
        number_products = len(df_product_details_tocrawl)

    make_reservation(df_product_details_tocrawl,number_products,preemptible_code,ip_address,marketplace,pre_instance_name, zone, api_key, chat_id)
    df_product_details_total = None
    asin_list = []
    bsr_list = []
    price_list = []

    for j, product_row in df_product_details_tocrawl.iloc[0:number_products].iterrows():
        asin = product_row["asin"]
        url_product = product_row["url_product"]
        url_product_asin = product_row["url_product_asin"]
        if True:
            # try to get reponse with free proxies
            response = get_response(marketplace, url_product_asin, api_key, chat_id, use_proxy=False, connection_timeout=connection_timeout, time_break_sec=time_break_sec, seconds_between_crawl=seconds_between_crawl)
        
            if response == None:
                # if script is called by preemptible instance it should be deleted by itself
                if pre_instance_name != "" and "pre" in pre_instance_name:
                    # update reservation logs with blacklist of ip 
                    update_reservation_logs(marketplace, "blacklist", "blacklist", preemptible_code, ip_address, "blacklist", "blacklist", pre_instance_name, zone, api_key, chat_id)
                    stop_instance(pre_instance_name, zone, "Response is none because of time break condition", api_key, chat_id)
                else:
                    assert response != None, "Could not get response within time break condition"

            if response == 404:
                crawlingdate = [datetime.datetime.now()]
                df_product_details = pd.DataFrame(data={"asin":[asin],"price":[404.0],"price_str":["404"],"bsr":[404],"bsr_str":["404"], "array_bsr": [["404"]], "array_bsr_categorie": [["404"]],"customer_review_score_mean":[404.0],"customer_review_score": ["404"],"customer_review_count": [404], "timestamp":crawlingdate})
                # transform date/timestamo columns to datetime objects
                df_product_details['timestamp'] = df_product_details['timestamp'].astype('datetime64')
                # TODO find better solution
                if type(df_product_details_total) == type(None):
                    df_product_details_total = df_product_details
                else:
                    df_product_details_total = df_product_details_total.append(df_product_details)
                #try:
                #    df_product_details.to_gbq("mba_" + marketplace + ".products_details_daily",project_id="mba-pipeline", if_exists="append")
                #except:
                #    stop_instance(pre_instance_name, zone)
                
                asin_list.append(asin)
                bsr_list.append(df_product_details.loc[0,"bsr"])
                price_list.append(df_product_details.loc[0,"price_str"])
                #update_reservation_logs(marketplace, asin, "404", preemptible_code, ip_address, "404", "404", pre_instance_name, zone)
                print("No Match: Got 404: %s | %s of %s" % (asin, j+1, number_products))
                continue 

            # transform html response to soup format
            soup = BeautifulSoup(utils.get_div_in_html(response.text, 'id="dp-container"'), 'html.parser')

        else:
            with open("data/mba_detail_page.html") as f:
                html_str = f.read()
                asin = "B086D9RL8Q"
                soup = BeautifulSoup(utils.get_div_in_html(html_str, 'id="dp-container"'), 'html.parser')
        try:
            df_product_details = get_product_detail_daily_df(soup, asin, url_product_asin, marketplace)
        except:
            utils.send_msg(chat_id, "Error while trying to get information for asin: " + str(asin), api_key)
            continue
        asin_list.append(asin)
        bsr_list.append(df_product_details.loc[0,"bsr"])
        price_list.append(df_product_details.loc[0,"price_str"])
        # save product information string locally
        with open("data/product_information.txt", "w") as f:
            f.write(df_product_details.loc[0,"product_information_html"])
            df_product_details = df_product_details.drop(['product_information_html'], axis=1)
            
        # save product information in storage
        timestamp = datetime.datetime.now()
        utils.upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", "data/product_information.txt", "logs/"+marketplace+"/product_information_daily/%s_%s_%s_"%(timestamp.year, timestamp.month, timestamp.day)+str(asin)+".txt" )
        # TODO find better solution
        if type(df_product_details_total) == type(None):
            df_product_details_total = df_product_details
        else:
            df_product_details_total = df_product_details_total.append(df_product_details)

        print("Match: Successfully crawled product: %s | %s of %s" % (asin, j+1, number_products))
    # update data in bigquery if batch is finished
    try:
        df_product_details_total.to_gbq("mba_" + marketplace + ".products_details_daily",project_id="mba-pipeline", if_exists="append")
    except:
        time.sleep(10)
        try:
            df_product_details_total.to_gbq("mba_" + marketplace + ".products_details_daily",project_id="mba-pipeline", if_exists="append")
        except:
            stop_instance(pre_instance_name, zone, "Can not update bigquery product detail daily table", api_key, chat_id)
    update_reservation_logs_list(marketplace, asin_list, "success", preemptible_code, ip_address, bsr_list, price_list, pre_instance_name, zone)

    global df_successfull_proxies
    if type(df_successfull_proxies) != type(None):
        print(df_successfull_proxies.iloc[0])
    
    # if script is called by preemptible instance it should be deleted by itself
    if pre_instance_name != "" and "pre" in pre_instance_name:
        stop_instance(pre_instance_name, zone, "Success", api_key, chat_id)


if __name__ == '__main__':
    main(sys.argv)

