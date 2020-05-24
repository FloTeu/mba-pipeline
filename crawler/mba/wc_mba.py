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
import random 
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

def get_asin_crawled(table_id):
    '''
        Returns a unique list of asins that are already crawled
    '''
    bq_client = bigquery.Client(project='mba-pipeline')
    # todo: change table name in future | 
    list_asin = bq_client.query("SELECT asin FROM " + table_id + " group by asin").to_dataframe()["asin"].tolist()
    return list_asin

def get_shirt_product_df(shirt_soup, asin_crawled_list, pages, url_mba):
    list_url_products = []
    list_url_images_lowq = []
    list_url_images_hq = []
    list_titles = []
    list_brands = []
    list_prices = []
    list_asin = []
    list_uuid = []
    list_crawlingdate = []

    asin_already_exist = False

    for i, shirt in enumerate(shirt_soup):
        try:
            # get asin
            asin = shirt.parent["data-asin"]
            # BREAK CONDITION only if pages parameter is not set
            if pages == 0 and asin in asin_crawled_list:
                asin_already_exist = True
                continue
            list_asin.append(asin)
            # get uuid
            list_uuid.append(shirt.parent["data-uuid"])
            # get urls
            image_div = shirt.find("div", class_="a-section a-spacing-none s-image-overlay-black")
            link = image_div.find("a")
            # product url
            url_product = "/".join(url_mba.split("/")[0:3]) + link['href']
            list_url_products.append(url_product)
            # images url
            url_image_lowq = link.find_all("img")[0]["src"]
            url_image_hq = link.find_all("img")[0]["srcset"].split(" ")[-2:len(link.find_all("img")[0]["srcset"].split(" "))-1][0]
            list_url_images_lowq.append(url_image_lowq)
            list_url_images_hq.append(url_image_hq)
            # price
            list_prices.append(shirt.find_all("span", class_="a-price-whole")[0].get_text())
            # brand name
            list_brands.append(shirt.find_all("h5", class_="s-line-clamp-1")[0].get_text())
            # brand name
            list_titles.append(shirt.find_all("a", class_="a-link-normal a-text-normal")[0].find("span").get_text())
            # timestamp
            list_crawlingdate.append(datetime.datetime.now())
        except:
            # exception is thrown if no tshirts are available
            break
    df_products = pd.DataFrame(data={"title":list_titles,"brand":list_brands,"url_product":list_url_products,"url_image_lowq":list_url_images_lowq,"url_image_hq":list_url_images_hq,"price":list_prices,"asin":list_asin,"uuid":list_uuid, "timestamp":list_crawlingdate}, dtype=np.object)
    df_products['timestamp'] = df_products['timestamp'].astype('datetime64')

    return df_products, asin_already_exist

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('keyword', help='Keyword that you like to query in mba', type=str)
    parser.add_argument('api_key', help='API key of proxycrawl', type=str)
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('pod_product', help='Name of Print on Demand product. I.e "shirt", "premium", "longsleeve", "sweatshirt", "hoodie", "popsocket", "kdp"', type=str)
    parser.add_argument('sort', help='What kind of sorting do you want?. I.e "best_seller", "price_up", "price_down", "cust_rating", "oldest", "newest"', type=str)
    parser.add_argument('--pages', default=0, type=int, help='Count of pages that shoul be crawled on amazon. Asin break condition is ignored if not 0')
    parser.add_argument('--start_page', default=1, type=int, help='Starting page number. Default is 1 (first page')

    print(os.getcwd())
    print(argv)
    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    keyword = args.keyword
    api_key = args.api_key
    marketplace = args.marketplace
    pod_product = args.pod_product
    sort = args.sort
    pages = args.pages
    start_page = args.start_page

    language = "de"
    
    # get all arguments
    args = parser.parse_args()

    #df = get_df_hobbies("de")
    df = pd.read_csv("data/hobbies_de.csv")
    hobbies_list = df["hobby"].tolist()
    test_hobby = hobbies_list[4]

    # get already crawled asin list
    asin_crawled_list = get_asin_crawled("mba_de.products")

    url_mba = url_creator.main([keyword, marketplace, pod_product, sort])

    # if start_page is other than zero, crawler should start from differnt page
    if start_page != 1:
        url_mba = url_mba + "&page="+str(start_page)+"&ref=sr_pg_"+str(start_page)

    #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
    #proxy_list = get_proxies("de", True)
    #proxy = next(iter(proxy_list))
    #proxies={"http": proxy, "https": proxy}

    # If pages are 0, i.e default value, loop shoul break if the first product apears which was already crawled
    # At least 30 pages should be crawled to prevent to much request with API
    if pages == 0:
        count_pages = 30
    else:
        count_pages = pages

    no_response = False
    for current_page in np.arange(start_page, start_page+count_pages, 1):
        #print(current_page)
        #'''
        timeout = time.time() + 60
        response = requests.get(utils.make_url_to_proxy_crawl_url(api_key,url_mba), stream=True)
        while response.status_code != 200:
            response = requests.get(utils.make_url_to_proxy_crawl_url(url_mba), stream=True)
            if time.time() > timeout:
                no_response = True
                break
        if no_response:
            print("Error: No response found. Status code: " + str(response.status_code))
            print("Current page: " + str(current_page))
            break
        else:
            print("Crawling mba data was successfull")
        # transform html response to soup format
        soup = BeautifulSoup(get_shirt_div(response.text, "s-main-slot s-result-list s-search-results sg-row"), 'html.parser')

        with open("data/mba_page.html", "w") as f:
            f.write(response.text)
        # save html page in storage
        utils.upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", "data/mba_page.html" , "logs/"+marketplace+"/product_overview/"+str(datetime.date.today())+"_"+keyword+"_"+sort+"_"+str(current_page)+".html")
        
        # use this code block to read html without appling proxy crawl
        #with open("data/newest_2.html") as f:
        #    html_str = f.read()
        #    soup = BeautifulSoup(get_shirt_div(html_str, "s-main-slot s-result-list s-search-results sg-row"), 'html.parser') 

        shirts = soup.find_all("div", class_="sg-col-inner")

        # get dataframe with product information
        df_products, asin_already_crawled = get_shirt_product_df(shirts, asin_crawled_list, pages, url_mba)

        # save data in big query
        df_products.to_gbq("mba_" + marketplace + ".products",project_id="mba-pipeline", if_exists="append")

        # get link to next page 
        url_mba = "/".join(url_mba.split("/")[0:3]) + soup.find("ul", class_="a-pagination").find(class_="a-last").find("a")["href"]
        
        print("Page " + str(current_page) + " successfully crawled")
        # BREAK CONDITION only if pages parameter is not set
        if pages == 0 and asin_already_crawled:
            break
        #'''
    bucket_name = "5c0ae2727a254b608a4ee55a15a05fb7"
    folder_name = "mba-shirts"
    file_path = "mba-pipeline/crawler/mba/data/test.jpg"
    #upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", file_path , "mba-shirts/test.jpg")

    
    test = 0

if __name__ == '__main__':
    main(sys.argv)

