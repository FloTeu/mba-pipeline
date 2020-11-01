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
import shutil
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
import multiprocessing
from mba_crawler.proxy import proxy_handler


def get_images_urls_not_crawled(marketplace):
    bq_client = bigquery.Client(project='mba-pipeline')
    df_images = bq_client.query("SELECT t0.asin, t0.url_image_hq, t0.url_image_lowq FROM mba_" + marketplace + ".products t0 LEFT JOIN mba_" + marketplace + ".products_images t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp").to_dataframe().drop_duplicates(["asin"])
    return df_images

def save_img(response, file_name):
    with open("mba-pipeline/crawler/mba/data/shirts/"+ file_name +".jpg", 'wb') as f:
        f.write(response.get_raw())

def get_asin_images_crawled(table_id):
    '''
        Returns a unique list of asins that are already crawled
    '''
    bq_client = bigquery.Client(project='mba-pipeline')
    # todo: change table name in future | 
    list_asin = bq_client.query("SELECT asin FROM " + table_id + " group by asin").to_dataframe()["asin"].tolist()
    return list_asin

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('--number_chunks', default=1, type=int, help='Number of images that shoul be crawled. If 0, every image that is not already crawled will be crawled.')
    parser.add_argument('--chunk_size', default=10, type=int, help='Chunk of images to batch upload to bigquery.')

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    number_chunks = args.number_chunks
    chunk_size = args.chunk_size
    
    # get all arguments
    args = parser.parse_args()

    # get already crawled asin list
    #asin_crawled_list = get_asin_images_crawled("mba_de.products_images")

    df_images = get_images_urls_not_crawled(marketplace)

    
    pool = multiprocessing.Pool(4)

    def crawl_img(image_row):
        asin = image_row["asin"]
        url_image_hq = image_row["url_image_hq"]
        print(asin)
        r = ProxyRequests(url_image_hq)
        r.get()
        print("Proxy used: " + str(r.get_proxy_used()))
        if 200 == r.get_status_code():
            print(r.get_status_code())
            # save image locally
            with open("data/shirts/shirt.jpg", 'wb') as f:
                f.write(r.get_raw())
            
            #df_img = pd.DataFrame(data={"asin":[asin],"url":["https://storage.cloud.google.com/5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/"+marketplace+"/"+asin+".jpg"],"url_gs":["gs://5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/"+marketplace+"/"+asin+".jpg"],"url_mba_lowq":[url_image_lowq],"url_mba_hq":[url_image_hq], "timestamp":[datetime.datetime.now()]}, dtype=np.object)
            #df_imgs = df_imgs.append(df_img)
            #utils.upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", "data/shirts/shirt.jpg", "mba-shirts/"+marketplace+"/" + asin + ".jpg")
            
            print("Successfully crawled image: %s" % (asin))
        else:
            print("Could not crawl image: %s" % (asin))

    df_images_chunks = [df_images[i:i+chunk_size] for i in range(0,df_images.shape[0],chunk_size)]

    # if number_images is equal to 0, evry image should be crawled
    if number_chunks == 0:
        number_chunks = len(df_images_chunks)

    for j, df_images in enumerate(df_images_chunks[0:number_chunks]):
        df_imgs = pd.DataFrame(data={"asin":[],"url":[],"url_gs":[],"url_mba_lowq":[],"url_mba_hq":[], "timestamp":[]}, dtype=np.object)
        #df_dask = ddf.from_pandas(df_images, npartitions=chunk_size)   # where the number of partitions is the number of cores you want to use
        #df_dask.apply(lambda x: crawl_img(x), meta=('str'), axis=1).compute(scheduler='multiprocessing')
        for i, image_row in df_images.iterrows():
            asin = image_row["asin"]
            url_image_hq = image_row["url_image_hq"]
            url_image_lowq = image_row["url_image_lowq"]

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
            #proxy_list = get_proxies("de", True)
            #proxy = next(iter(proxy_list))
            proxies=proxy_handler.get_random_proxy_url_dict(path_proxy_json='mba_crawler/proxy/proxies.json')
            r = requests.get(url_image_hq,proxies=proxies,headers=headers, stream=True)
            #print("Proxy used: " + str(r.meta))

            #r = ProxyRequests(url_image_hq)
            #r.get()
            #print("Proxy used: " + str(r.get_proxy_used()))
            if 200 == r.status_code:
                # save image locally
                with open("data/shirts/shirt.jpg", 'wb') as f:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, f) 
                
                df_img = pd.DataFrame(data={"asin":[asin],"url":["https://storage.cloud.google.com/5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/"+marketplace+"/"+asin+".jpg"],"url_gs":["gs://5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/"+marketplace+"/"+asin+".jpg"],"url_mba_lowq":[url_image_lowq],"url_mba_hq":[url_image_hq], "timestamp":[datetime.datetime.now()]}, dtype=np.object)
                df_imgs = df_imgs.append(df_img)
                utils.upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", "data/shirts/shirt.jpg", "mba-shirts/"+marketplace+"/" + asin + ".jpg", verbose=False)
            else:
                print("Could not crawl image: %s" % (asin))
            
            #response = requests.get(quote_plus(url_image_hq),proxies=proxies,headers=headers, stream=True)
            test = 0
        print("%s of %s chunks" % (asin, j+1, number_chunks))
        df_imgs['timestamp'] = df_imgs['timestamp'].astype('datetime64')
        df_imgs.to_gbq("mba_" + marketplace + ".products_images",project_id="mba-pipeline", if_exists="append")
        test = 0

    bucket_name = "5c0ae2727a254b608a4ee55a15a05fb7"
    folder_name = "mba-shirts"
    file_path = "mba-pipeline/crawler/mba/data/test.jpg"
    #upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", file_path , "mba-shirts/test.jpg")

    
    test = 0

if __name__ == '__main__':
    main(sys.argv)

