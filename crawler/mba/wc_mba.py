from bs4 import BeautifulSoup
import requests 
from requests_html import HTMLSession
import pandas as pd
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
import mba_url_creator as url_creator
import random 
from lxml.html import fromstring
from itertools import cycle
import datetime 
import shutil 
from google.cloud import storage

def get_proxies(country="de", https_only=True):
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    if https_only:
        condition_str = './/td[7][contains(text(),"yes")]'
    else:
        condition_str = True
    for i in parser.xpath('//tbody/tr')[:400]:
        if condition_str == True or i.xpath(condition_str):
            if country == "de" and i.xpath('.//td[3]/text()')[0] == "DE" and i.xpath('.//td[5]/text()')[0] not in ["transparent", "anonymous"]:
                #Grabbing IP and corresponding PORT
                proxy=":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
            if country == "com" and i.xpath('.//td[3]/text()')[0] == "US" and i.xpath('.//td[5]/text()')[0] not in ["transparent", "anonymous"]:
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
            if country=="uk" and i.xpath('.//td[3]/text()')[0] == "GB" and i.xpath('.//td[5]/text()')[0] not in ["transparent", "anonymous"]:
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
    return proxies

def get_random_user_agent():
    user_agent_list = [
   #Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
    ]
    #user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    return random.choice(user_agent_list)

def make_url_to_proxy_crawl_url(url_mba):
    url = quote_plus(url_mba)
    url_proxycrawl = 'https://api.proxycrawl.com/?token=PlhAyiU_2cQukrs_BZTuiQ&url=' + url
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

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('keyword', help='Keyword that you like to query in mba', type=str)
    parser.add_argument('api_key', help='API key of proxycrawl', type=str)
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('pod_product', help='Name of Print on Demand product. I.e "shirt", "premium", "longsleeve", "sweatshirt", "hoodie", "popsocket", "kdp"', type=str)
    parser.add_argument('sort', help='What kind of sorting do you want?. I.e "best_seller", "price_up", "price_down", "cust_rating", "oldest", "newest"', type=str)

    if len(argv) == 6:
        argv = argv[1:6]

    # get all arguments
    args = parser.parse_args(argv)
    keyword = args.keyword
    api_key = args.api_key
    marketplace = args.marketplace
    pod_product = args.pod_product
    sort = args.sort

    language = "de"
    
    # get all arguments
    args = parser.parse_args()

    #df = get_df_hobbies("de")
    df = pd.read_csv("~/mba-pipeline/crawler/mba/data/hobbies_de.csv")
    hobbies_list = df["hobby"].tolist()
    test_hobby = hobbies_list[4]

    url_mba = url_creator.main([keyword, marketplace, pod_product, sort])

    #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
    #proxy_list = get_proxies("de", True)
    #proxy = next(iter(proxy_list))
    #proxies={"http": proxy, "https": proxy}

    #response = requests.get(make_url_to_proxy_crawl_url(url_mba), stream=True)
    #soup = BeautifulSoup(response.content, 'html.parser')

    #with open("mba-pipeline/crawler/mba/data/newest_2.html", "w") as f:
    #    f.write(response.text)

    with open("mba-pipeline/crawler/mba/data/newest_2.html") as f:
        html_str = f.read()
        soup = BeautifulSoup(get_shirt_div(html_str, "s-main-slot s-result-list s-search-results sg-row"), 'html.parser')

    shirts = soup.find_all("div", class_="sg-col-inner")

    list_url_products = []
    list_url_images_lowq = []
    list_url_images_hq = []
    list_titles = []
    list_brands = []
    list_prices = []
    list_asin = []
    list_uuid = []
    list_crawlingdate = []
    for i, shirt in enumerate(shirts):
        try:
            # get asin
            list_asin.append(shirt.parent["data-asin"])
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
            # bramd name
            list_titles.append(shirt.find_all("a", class_="a-link-normal a-text-normal")[0].find("span").get_text())
            # timestamp
            list_crawlingdate.append(datetime.datetime.now())
        except:
            # exception is thrown if no tshirts are available
            break
    df_products = pd.DataFrame(data={"title":list_titles,"brand":list_brands,"url_product":list_url_products,"url_image_lowq":list_url_images_lowq,"url_image_hq":list_url_images_hq,"price":list_prices,"asin":list_asin,"uuid":list_uuid, "timestamp":list_crawlingdate})
    # save data in big query
    df_products.to_gbq("mba.product_newest",project_id="mba-pipeline", if_exists="append")
    
    bucket_name = "5c0ae2727a254b608a4ee55a15a05fb7"
    folder_name = "mba-shirts"
    file_path = "mba-pipeline/crawler/mba/data/test.jpg"
    #upload_blob("5c0ae2727a254b608a4ee55a15a05fb7", file_path , "mba-shirts/test.jpg")

    
    test = 0

if __name__ == '__main__':
    main(sys.argv)

