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
from utils import get_df_hobbies
import utils
import mba_url_creator as url_creator
from lxml.html import fromstring
from itertools import cycle
import datetime 
import shutil 
from google.cloud import storage
from google.cloud import bigquery
import os
import time 

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

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project='mba-pipeline')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def get_asin_product_detail_crawled(marketplace):
    bq_client = bigquery.Client(project='mba-pipeline')
    df_prduct_details = bq_client.query("SELECT t0.asin, t0.url_image_hq, t0.url_image_lowq FROM mba_" + marketplace + ".products t0 LEFT JOIN mba_" + marketplace + ".products_details t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp").to_dataframe().drop_duplicates()
    return df_prduct_details




def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('api_key', help='API key of proxycrawl', type=str)
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--number_images', default=10, type=int, help='Number of images that shoul be crawled. If 0, every image that is not already crawled will be crawled.')

    print(os.getcwd())
    print(argv)
    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    api_key = args.api_key
    marketplace = args.marketplace

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    number_images = args.number_images
    
    # get all arguments
    args = parser.parse_args()

    # get already crawled asin list
    #asin_crawled_list = get_asin_images_crawled("mba_de.products_images")

    df_images = get_asin_product_detail_crawled(marketplace)

    # if number_images is equal to 0, evry image should be crawled
    if number_images == 0:
        number_images = len(df_images)

    for j, image_row in df_images.iloc[0:number_images].iterrows():
        asin = image_row["asin"]
        url_image_hq = image_row["url_image_hq"]
        url_image_lowq = image_row["url_image_lowq"]

        #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
        #proxy_list = get_proxies("de", True)
        #proxy = next(iter(proxy_list))
        #proxies={"http": proxy, "https": proxy}

        r = ProxyRequests(url_image_hq)
        r.get()
        print("Proxy used: " + str(r.get_proxy_used()))
        if 200 == r.get_status_code():
            print(r.get_status_code())
            # save image locally
            with open("data/shirts/shirt.jpg", 'wb') as f:
                f.write(r.get_raw())

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

