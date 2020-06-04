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

def make_url_to_proxy_crawl_url(api_key, url_mba):
    url = quote_plus(url_mba)
    url_proxycrawl = 'https://api.proxycrawl.com/?token='+api_key+'&url=' + url
    return url_proxycrawl

def get_div_in_html(html_str, div_class_or_id):
    html_for_bs = ""
    count_div = 0
    start_saving_html = False
    html_tag = ""
    start_get_tag = False
    html_tag_finished = ""
    
    assert not "captcha" in html_str.lower(), "Captcha is requested. Crawling will be stoped"

    for char in html_str:
        
        if div_class_or_id in html_tag and char == ">":
            start_saving_html = True
        
        html_tag = html_tag + char
        if char == "<":
            start_get_tag = True
        if char == ">":
            html_tag_finished = html_tag
            start_get_tag = False
            html_tag = ""
        
        # if div is opening div count is increasing by one
        if "<div" in html_tag_finished and start_saving_html:
            count_div += 1
        # if div is opening div count is decreasing by one
        if "</div" in html_tag_finished and start_saving_html:
            count_div -= 1
        # as long as initial parent div is not closed we fill out html str  
        if start_saving_html and html_for_bs == "":
            html_for_bs += html_tag_finished
        elif start_saving_html:
            html_for_bs += char

        # Breaking condition if closing div is reached
        if start_saving_html and count_div == 0:
            html_for_bs = html_for_bs[1:len(html_for_bs)]
            break

        html_tag_finished = ""
    
    assert html_for_bs != "", "HTML does not contains: " + div_class_or_id

    return html_for_bs

def save_img(response, file_name):
    with open("mba-pipeline/crawler/mba/data/"+ file_name +".jpg", 'wb') as f:
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, f) 

def get_asin_product_detail_crawled(marketplace):
    project_id = 'mba-pipeline'
    reservationdate = datetime.datetime.now()
    dataset_id = "preemptible_logs"
    table_id = "mba_detail_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day)
    reservation_table_id = dataset_id + "." + table_id
    bq_client = bigquery.Client(project=project_id)
    df_product_details = bq_client.query("SELECT t0.asin, t0.url_product FROM mba_" + marketplace + ".products t0 LEFT JOIN mba_" + marketplace + ".products_details t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp").to_dataframe().drop_duplicates()
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
    weight = [""]
    upload_date = [""]
    customer_recession_score = [""]
    customer_recession_count = [""]
    mba_bsr_str = [""]
    array_mba_bsr = []
    array_mba_bsr_categorie = []

    for info in list_product_information:
        info_text = info.get_text().lower()
        if "gewicht" in info_text or "weight" in info_text:
            weight = [info.get_text()]
        if "seit" in info_text or "available" in info_text:
            upload_date = [info.get_text()]
        if "rezension" in info_text or "review" in info_text:
            try:
                customer_recession_score = [info.find("span", class_="a-declarative").find("a").find("i").get_text()]
                customer_recession_count = [int(info.find("span", class_="a-size-small").find("a").get_text().split(" ")[0])]
            except:
                customer_recession_score = [""]
                customer_recession_count = [0]
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

        except:
            pass

    return weight, upload_date, customer_recession_score, customer_recession_count, mba_bsr_str, array_mba_bsr, array_mba_bsr_categorie

def get_product_information(marketplace, list_product_information):
    if marketplace == "de":
        return get_product_information_de(list_product_information)
    else:
        raise("Marketplace not known")


def get_product_detail_df(soup, asin, url_mba, marketplace):
    product_feature = soup.find("div", id="feature-bullets")
    product_description = soup.find("div", id="productDescription_feature_div")
    product_information = soup.find("div", id="detail-bullets_feature_div")
    
    # get all headline infos 
    title = [soup.find("span", id="productTitle").get_text().replace("\n","").replace("                                                                                                                                                        ","").replace("                                                                                                                        ", "")]
    brand = [soup.find("a", id="bylineInfo").get_text()]
    url_brand = ["/".join(url_mba.split("/")[0:3]) + soup.find("a", id="bylineInfo")["href"]]
    price = [soup.find("span", id="priceblock_ourprice").get_text()]

    array_fit_types = []
    div_fit_types = soup.find("div", id="variation_fit_type").find_all("span", class_="a-size-base")
    # found more than one fit type
    if len(div_fit_types) > 0:
        for fit_type in div_fit_types:
            array_fit_types.append(fit_type.get_text())
    else:
        array_fit_types.append(soup.find("div", id="variation_fit_type").find("span").get_text())


    array_color_names = []
    span_color_names = soup.find("div", id="variation_color_name").find_all("span", class_="a-declarative")
    if len(span_color_names) > 0:
        for color_name in span_color_names:
            array_color_names.append(color_name.find("img")["alt"])
    else:
        array_color_names.append(soup.find("div", id="variation_color_name").find("span").get_text())
    color_count = [len(array_color_names)]

    array_product_features = []
    for feature in product_feature.find("ul").find_all("li"):
        array_product_features.append(feature.get_text().replace("\n","").replace("\t",""))

    # get product description
    try:
        product_description = [product_description.find("div", id="productDescription").get_text().replace("\n", "")]
    except:
        product_description = [""]

    # get all product information
    list_product_information = product_information.find("ul").find_all("li")
    weight, upload_date_str, customer_recession_score, customer_recession_count, mba_bsr_str, array_mba_bsr, array_mba_bsr_categorie = get_product_information(marketplace, list_product_information)
    
    # try to get real upload date
    upload_date = [dateparser.parse(upload_date_str[0].split(":")[1]).strftime('%Y-%m-%d')]

    crawlingdate = [datetime.datetime.now()]

    df_products_details = pd.DataFrame(data={"asin":[asin],"title":title,"brand":brand,"url_brand":url_brand,"price":price,"fit_types":[array_fit_types],"color_names":[array_color_names],"color_count":color_count,"product_features":[array_product_features],"description":product_description,"weight": weight,"upload_date_str":upload_date_str,"upload_date": upload_date,"customer_review_score": customer_recession_score,"customer_review_count": customer_recession_count,"mba_bsr_str": mba_bsr_str, "mba_bsr": [array_mba_bsr], "mba_bsr_categorie": [array_mba_bsr_categorie], "timestamp":crawlingdate})
    # transform date/timestamo columns to datetime objects
    df_products_details['timestamp'] = df_products_details['timestamp'].astype('datetime64')
    df_products_details['upload_date'] = df_products_details['upload_date'].astype('datetime64')

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
def get_response(marketplace, url_product_asin, use_proxy=True, connection_timeout=5.0,time_break_sec=60, seconds_between_crawl=20):
    time_start = time.time()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
    
    
    # set global variables
    global proxy_list
    global country_list
    global last_successfull_crawler
    global time_since_last_crawl
    global df_successfull_proxies

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
    
    def set_error_data(proxy, message):
        global df_successfull_proxies
        # add error if preveasly successfull proxy was used
        if type(df_successfull_proxies) != type(None) and proxy in df_successfull_proxies["proxy"].tolist():
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
            wait_seconds = seconds_between_crawl - (time.time()-time_since_last_crawl)
            print("Same proxie used, wait for %.2f seconds" % wait_seconds)
            time.sleep(wait_seconds)

        try:
            headers = utils.get_random_headers(marketplace)
            # try to get response
            if use_proxy:
                response = requests.get(url_product_asin, timeout=connection_timeout, proxies=proxies, headers=headers)#, verify=False)
            # use gcp ip instead of proxy
            else:
                response = requests.get(url_product_asin, timeout=connection_timeout, headers=headers)
            if response.status_code == 200:
                if "captcha" in response.text.lower():
                    print("No Match: Got code 200, but captcha is requested. User agent: %s. Try next proxy... (Country: %s)" % (headers['user-agent'],country))
                    set_error_data(proxy, "captcha")
                    reset_proxies_and_country_list()
                    continue
                # successfull crawl
                else:
                    # start global time variable to check duration between successfull crawling with same proxie
                    time_since_last_crawl = time.time()
                    last_successfull_crawler = proxy
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
                    
                    print("Match: Scrape successfull in %.2f seconds (Country: %s)" % ((time.time() - time_start), country))
                    return response
            else:
                #TODO: If status code is 404, Product was probably removed because of violation of law
                # Save that information
                print("No Match: Status code: " + str(response.status_code) + ", user agent: %s (Country: %s)" % (headers['user-agent'], country))
                if response.status_code == 404:
                    print("Url not found. Product was probably removed")
                    return 404
                set_error_data(proxy, "Status code: " + str(response.status_code))
                reset_proxies_and_country_list()
                continue
        except Exception as e:
            print("No Match: got exception: %s (Country: %s)" % (type(e).__name__, country))
            #print(str(e))
            set_error_data(proxy, "Exception: " + str((type(e).__name__)))
            reset_proxies_and_country_list()
            continue

    # return None if no response could be crawled
    return None

def update_reservation_logs(marketplace, asin, status, preemptible_code, ip_address):
    global df_successfull_proxies
    error_str = ""
    if type(df_successfull_proxies) != type(None):
        print(df_successfull_proxies.iloc[0])
        error_str = "Error count: " + str(df_successfull_proxies.iloc[0]["errorCount"]) + " errors: " + ",".join(df_successfull_proxies.iloc[0]["errors"])

    reservationdate = datetime.datetime.now()
    df_reservation = pd.DataFrame({"asin": [str(asin)], "timestamp": [reservationdate], "status": [str(status)], "pree_id": [str(preemptible_code)], "ip_address":[ip_address], "error_log": [error_str]})
    df_reservation['timestamp'] = df_reservation['timestamp'].astype('datetime64')
    df_reservation.to_gbq("preemptible_logs.mba_detail_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day),project_id="mba-pipeline", if_exists="append")

def stop_instance(pre_instance_name, zone):
    bashCommand = "yes Y | gcloud compute instances stop {} --zone {}".format(pre_instance_name, zone)
    stream = os.popen(bashCommand)
    output = stream.read()

def get_extrenal_ip(pre_instance_name, zone):
    bashCommand = "yes Y | gcloud compute instances describe {} --zone {}  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'".format(pre_instance_name, zone)
    stream = os.popen(bashCommand)
    ip_address = stream.read()
    return ip_address.replace("\n", "")

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--number_products', default=10, type=int, help='Number of products/shirts that shoul be crawled. If 0, every image that is not already crawled will be crawled.')
    parser.add_argument('--connection_timeout', default=10.0, type=float, help='Time that the request operation has until its breaks up. Default: 10.0 sec')
    parser.add_argument('--time_break_sec', default=240, type=int, help='Time in seconds the script tries to get response of certain product. Default 240 sec')
    parser.add_argument('--seconds_between_crawl', default=20, type=int, help='Time in seconds in which no proxy/ip shoul be used twice for crawling. Important to prevent being blacklisted. Default 20 sec')
    parser.add_argument('--preemptible_code', default="0", type=str, help='Identifier of instance for pree logs. Default 0 which leads to GUID.')
    parser.add_argument('--pre_instance_name', default="", type=str, help='Name of instance. Important: if set, script will stop instance after successfull operation. Default "".')

    print(os.getcwd())
    print(argv)

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    number_products = args.number_products
    connection_timeout = args.connection_timeout
    time_break_sec = args.time_break_sec
    seconds_between_crawl = args.seconds_between_crawl
    preemptible_code = args.preemptible_code
    pre_instance_name = args.pre_instance_name
    zone = utils.get_zone_of_marketplace(marketplace)
    ip_address = get_extrenal_ip(pre_instance_name, zone)

    if preemptible_code == "0":
        preemptible_code = uuid.uuid4().hex

    # get all arguments
    args = parser.parse_args()

    # get asins which are not already crawled
    df_product_details_tocrawl = get_asin_product_detail_crawled(marketplace)
    #df_product_details = pd.DataFrame(data={"asin": ["B07RVNJHZL"], "url_product": ["adwwadwad"]})
    df_product_details_tocrawl["url_product_asin"] =  df_product_details_tocrawl.apply(lambda x: "https://www.amazon."+marketplace+"/dp/"+x["asin"], axis=1)
    
    # if number_images is equal to 0, evry image should be crawled
    if number_products == 0:
        number_products = len(df_product_details_tocrawl)

    reservationdate = datetime.datetime.now()
    df_reservation = df_product_details_tocrawl.iloc[0:number_products][["asin"]].copy()
    df_reservation['status'] = "blocked"
    df_reservation['pree_id'] = preemptible_code
    df_reservation['ip_address'] = ip_address
    df_reservation['error_log'] = ""
    df_reservation['timestamp'] = reservationdate
    df_reservation['timestamp'] = df_reservation['timestamp'].astype('datetime64')
    df_reservation.to_gbq("preemptible_logs.mba_detail_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day),project_id="mba-pipeline", if_exists="append")

    for j, product_row in df_product_details_tocrawl.iloc[0:number_products].iterrows():
        asin = product_row["asin"]
        url_product = product_row["url_product"]
        url_product_asin = product_row["url_product_asin"]
    
        if True:
            # try to get reponse with free proxies
            response = get_response(marketplace, url_product_asin, use_proxy=False, connection_timeout=connection_timeout, time_break_sec=time_break_sec, seconds_between_crawl=seconds_between_crawl)
        
            if response == None:
                # if script is called by preemptible instance it should be deleted by itself
                if pre_instance_name != "":
                    stop_instance(pre_instance_name, zone)
                else:
                    assert response != None, "Could not get response within time break condition"

            if response == 404:
                crawlingdate = [datetime.datetime.now()]
                df_product_details = pd.DataFrame(data={"asin":[asin],"title":["404"],"brand":["404"],"url_brand":["404"],"price":["404"],"fit_types":[["404"]],"color_names":[["404"]],"color_count":[404],"product_features":[["404"]],"description":["404"],"weight": ["404"],"upload_date_str":["1995-01-01"],"upload_date": ["1995-01-01"],"customer_review_score": ["404"],"customer_review_count": [404],"mba_bsr_str": ["404"], "mba_bsr": [["404"]], "mba_bsr_categorie": [["404"]], "timestamp":crawlingdate})
                # transform date/timestamo columns to datetime objects
                df_product_details['timestamp'] = df_product_details['timestamp'].astype('datetime64')
                df_product_details['upload_date'] = df_product_details['upload_date'].astype('datetime64')
                df_product_details.to_gbq("mba_" + marketplace + ".products_details",project_id="mba-pipeline", if_exists="append")
                update_reservation_logs(marketplace, asin, "404", preemptible_code, ip_address)
                print("No Match: Got 404: %s | %s of %s" % (asin, j+1, number_products))
                continue 

            # save product detail page locally
            with open("data/mba_detail_page.html", "w") as f:
                f.write(response.text)

            # transform html response to soup format
            soup = BeautifulSoup(get_div_in_html(response.text, 'id="dp-container"'), 'html.parser')
            
            # save html in storage
            utils.upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", "data/mba_detail_page.html", "logs/"+marketplace+"/product_detail/"+str(asin)+".html")
        else:
            with open("data/mba_detail_page.html") as f:
                html_str = f.read()
                asin = "B086D9RL8Q"
                soup = BeautifulSoup(get_div_in_html(html_str, 'id="dp-container"'), 'html.parser') 

        df_product_details = get_product_detail_df(soup, asin, url_product_asin, marketplace)
        df_product_details.to_gbq("mba_" + marketplace + ".products_details",project_id="mba-pipeline", if_exists="append")
        update_reservation_logs(marketplace, asin, "success", preemptible_code, ip_address)
        print("Match: Successfully crawled product: %s | %s of %s" % (asin, j+1, number_products))

    global df_successfull_proxies
    if type(df_successfull_proxies) != type(None):
        print(df_successfull_proxies.iloc[0])
    #df_successfull_proxies.to_csv("data/successfull_proxies.csv")
    
    # if script is called by preemptible instance it should be deleted by itself
    if pre_instance_name != "" and "pre" in pre_instance_name:
        stop_instance(pre_instance_name, zone)

    test = 0

if __name__ == '__main__':
    main(sys.argv)

