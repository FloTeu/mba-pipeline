from bs4 import BeautifulSoup
import requests 
from requests_html import HTMLSession
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

def get_shirt_div(html_str, div_class):
    html_for_bs = ""
    count_div = 0
    start_saving = False
    for line in html_str.split("\n"):
        if div_class in line:
            count_div += 1
            start_saving = True
        # if div is opening div count is increasing by one
        if "<div" in line:
            count_div += 1
        # if div is opening div count is decreasing by one
        if "</div" in line:
            count_div -= 1
        # as long as initial parent div is not closed we fill out html str  
        if start_saving:
            html_for_bs += line
        # Breaking condition if closing div is reached
        if start_saving and count_div == 0:
            break
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

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
        headers = {
        'HOST': 'www.amazon.' + marketplace,
        'authority': 'www.amazon.' + marketplace,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'cookie': '__utma=12798129.504353392.1590337669.1590337669.1590337669.1; __utmc=12798129; __utmz=12798129.1590337669.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utmb=12798129.1.10.1590337669',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'sec-fetch-user': '?1',
        'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',}

        proxy_list = utils.get_proxies("de", True)
        proxy = next(iter(proxy_list))
        proxies={"http": proxy, "https": proxy}

        response = ProxyRequests(url_product_asin)
        response.set_headers(headers)
        response.get_with_headers()
        print("Proxy used: " + str(response.get_proxy_used()))
        print(response.get_status_code())
        
        '''
        proxy_list = utils.get_proxies("de", True)
        for proxy in proxy_list:
            print(proxy)
            proxies={"http": proxy, "https": proxy}
            try:
                response = requests.get(url_product_asin, proxies=proxies, headers=headers)
                if response.status_code == 200:
                    break
            except:
                continue
            
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
        
        #response = requests.get(quote_plus(url_image_hq),proxies=proxies,headers=headers, stream=True)
        test = 0

    bucket_name = "5c0ae2727a254b608a4ee55a15a05fb7"
    folder_name = "mba-shirts"
    file_path = "mba-pipeline/crawler/mba/data/test.jpg"
    #upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", file_path , "mba-shirts/test.jpg")

    
    test = 0

if __name__ == '__main__':
    main(sys.argv)

