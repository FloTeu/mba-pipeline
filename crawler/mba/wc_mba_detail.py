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

def make_url_to_proxy_crawl_url(api_key, url_mba):
    url = quote_plus(url_mba)
    url_proxycrawl = 'https://api.proxycrawl.com/?token='+api_key+'&url=' + url
    return url_proxycrawl

def get_shirt_div(html_str, div_class_or_id):
    html_for_bs = ""
    count_div = 0
    start_saving_html = False
    html_tag = ""
    start_get_tag = False
    html_tag_finished = ""

    for char in html_str:
        
        if div_class_or_id in html_tag and char == ">":
            print("Found key word in: " + html_tag)
            start_saving_html = True

        if char == "<":
            start_get_tag = True
        if char == ">":
            html_tag_finished = html_tag
            start_get_tag = False
            html_tag = ""
        if start_saving_html:
            html_tag = html_tag + char
        
        # if div is opening div count is increasing by one
        if "<div" in html_tag_finished and start_saving_html:
            print("opened: " + html_tag_finished)
            count_div += 1
        # if div is opening div count is decreasing by one
        if "</div" in html_tag_finished and start_saving_html:
            print("finished: " + html_tag_finished)
            count_div -= 1
        # as long as initial parent div is not closed we fill out html str  
        if start_saving_html:
            html_for_bs += char
        # Breaking condition if closing div is reached
        if start_saving_html and count_div == 0:
            html_for_bs = html_for_bs[1:len(html_for_bs)]
            break

        html_tag_finished = ""

    return html_for_bs

def save_img(response, file_name):
    with open("mba-pipeline/crawler/mba/data/"+ file_name +".jpg", 'wb') as f:
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, f) 

def get_asin_product_detail_crawled(marketplace):
    bq_client = bigquery.Client(project='mba-pipeline')
    df_product_details = bq_client.query("SELECT t0.asin, t0.url_product FROM mba_" + marketplace + ".products t0 LEFT JOIN mba_" + marketplace + ".products_details t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp").to_dataframe().drop_duplicates()
    return df_product_details


def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('api_key', help='API key of proxycrawl', type=str)
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--number_products', default=10, type=int, help='Number of products/shirts that shoul be crawled. If 0, every image that is not already crawled will be crawled.')

    print(os.getcwd())
    print(argv)

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    number_products = args.number_products
    
    # get all arguments
    args = parser.parse_args()

    # get already crawled asin list
    #asin_crawled_list = get_asin_images_crawled("mba_de.products_images")

    df_product_details = get_asin_product_detail_crawled(marketplace)
    df_product_details["url_product_asin"] =  df_product_details.apply(lambda x: "https://www.amazon."+marketplace+"/dp/"+x["asin"], axis=1)

    # if number_images is equal to 0, evry image should be crawled
    if number_products == 0:
        number_products = len(df_product_details)

    for j, product_row in df_product_details.iloc[0:number_products].iterrows():
        asin = product_row["asin"]
        url_product = product_row["url_product"]
        url_product_asin = product_row["url_product_asin"]

        if False:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
            headers = utils.get_random_headers(marketplace)

            proxy_list = utils.get_proxies(["de"], True)
            for proxy in proxy_list:
                print(proxy)
                proxies={"http": 'http://' + proxy, "https": 'https://' + proxy}
                try:
                    response = requests.get(url_product_asin, timeout=3.0, proxies=proxies, headers=headers)
                    if response.status_code == 200:
                        print("scrape successfull")
                        break
                except:
                    print("scrape not successfull")
                    continue
            
            # transform html response to soup format
            soup = BeautifulSoup(get_shirt_div(response.text, "dp"), 'html.parser')
            
            # save product detail page locally
            with open("data/mba_detail_page.html", "w") as f:
                f.write(response.text)

            # save html in storage
            utils.upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", "data/mba_detail_page.html", "logs/"+marketplace+"/product_detail/"+str(asin)+".html")
        else:    
            with open("data/mba_detail_page.html") as f:
                html_str = f.read()
                soup = BeautifulSoup(get_shirt_div(html_str, "s-main-slot s-result-list s-search-results sg-row"), 'html.parser') 

        print(response.status_code)
        '''
        print("Proxy used: " + str(response.get_proxy_used()))
        if 200 == response.get_status_code():
            print(response.get_status_code())
            # save image locally
            with open("data/mba_detail_page.html", "wb") as f:
                f.write(response.get_raw())

            utils.upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", "data/shirts/shirt.jpg", "mba-shirts/"+marketplace+"/" + asin + ".jpg")
            df_img = pd.DataFrame(data={"asin":[asin],"url":["https://storage.cloud.google.com/5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/"+marketplace+"/"+asin+".jpg"],"url_gs":["gs://5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/"+marketplace+"/"+asin+".jpg"],"url_mba_lowq":[url_image_lowq],"url_mba_hq":[url_image_hq], "timestamp":[datetime.datetime.now()]}, dtype=np.object)
            df_img['timestamp'] = df_img['timestamp'].astype('datetime64')
            df_img.to_gbq("mba_" + marketplace + ".products_images",project_id="mba-pipeline", if_exists="append")
            print("Successfully crawled image: %s | %s of %s" % (asin, j+1, number_images))
        else:
            print("Could not crawl image: %s | %s of %s" (asin, j+1, number_images))
        '''
        #response = requests.get(quote_plus(url_image_hq),proxies=proxies,headers=headers, stream=True)
        test = 0

    bucket_name = "5c0ae2727a254b608a4ee55a15a05fb7"
    folder_name = "mba-shirts"
    file_path = "mba-pipeline/crawler/mba/data/test.jpg"
    #upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", file_path , "mba-shirts/test.jpg")

    
    test = 0

if __name__ == '__main__':
    main(sys.argv)

