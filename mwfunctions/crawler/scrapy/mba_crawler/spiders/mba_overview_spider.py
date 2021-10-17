import scrapy
import json
import datetime
from pathlib import Path
from mwfunctions.crawler.proxy import proxy_handler
import pandas as pd
import numpy as np
from google.cloud import bigquery
from typing import List
from re import findall
import re
from bs4 import BeautifulSoup
import sys
sys.path.append("...")
sys.path.append("..")
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\flori\\Dropbox\\Apps\\MBA Pipeline\\merchwatch.de\\privacy files\\mba-pipeline-4de1c9bf6974.json"
#from proxy.utils import get_random_headers, send_msg
#from proxy import proxy_handler
from ..items import MbaCrawlerItem
from urllib.parse import urlparse
from scrapy.exceptions import CloseSpider
#import mba_url_creator as url_creator
import time
import os

# from scrapy.contrib.spidermiddleware.httperror import HttpError
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, ConnectionLost
from twisted.web._newclient import ResponseNeverReceived
from scrapy.core.downloader.handlers.http11 import TunnelError

from mwfunctions.crawler.proxy.utils import get_random_headers, send_msg
from mwfunctions.crawler.proxy import proxy_handler
from mwfunctions.crawler.scrapy.spider_base import MBAOverviewSpider
import mwfunctions.crawler.mba.url_creator as url_creator
from mwfunctions.pydantic.crawling_classes import MBAImageItems, MBAImageItem
from mwfunctions.pydantic.bigquery_classes import BQMBAOverviewProduct, BQMBAProductsMBAImages, BQMBAProductsMBARelevance


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ValueError("Provided argument is not a bool")

class MBAShirtOverviewSpider(MBAOverviewSpider):
    name = "mba_overview"
    website_crawling_target = "overview"
    Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
    df_products = pd.DataFrame(data={"title":[],"brand":[],"url_product":[],"url_image_lowq":[],"url_image_hq":[],"price":[],"asin":[],"uuid":[], "timestamp":[]})
    df_mba_images = pd.DataFrame(data={"asin":[],"url_image_lowq":[],"url_image_q2":[], "url_image_q3":[], "url_image_q4":[],"url_image_hq":[], "timestamp":[]})
    df_mba_relevance = pd.DataFrame(data={"asin":[],"sort":[],"number":[],"timestamp":[]})
    df_search_terms = pd.DataFrame()
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []
    captcha_count = 0
    page_count = 0
    shirts_per_page = 48
    change_zip_code_post_data = {
            'locationType': 'LOCATION_INPUT',
            'zipCode': '90210',
            'storeContext': 'apparel',
            'deviceType': 'web',
            'pageType': 'Search',
            'actionSource': 'glow'
            }
 
    # custom_settings = {
    #     # Set by settings.py
    #     #"ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=False),
    #
    #     'ITEM_PIPELINES': {
    #         'mba_crawler.pipelines.MbaCrawlerItemPipeline': 100,
    #         'mba_crawler.pipelines.MbaCrawlerImagePipeline': 200
    #     },
    #
    #     'IMAGES_STORE': 'gs://5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/',
    #     'GCS_PROJECT_ID': 'mba-pipeline'
    # }

    def __init__(self, marketplace, pod_product, sort, keyword="", pages=0, start_page=1, csv_path="", debug=True, **kwargs):
        super(MBAShirtOverviewSpider, self).__init__(marketplace, sort, **kwargs)

        self.marketplace = marketplace
        self.pod_product = pod_product
        self.keyword = keyword
        self.pages = int(pages)
        self.start_page = int(start_page)
        self.allowed_domains = ['amazon.' + marketplace]
        self.products_already_crawled = self.get_asin_crawled("mba_%s.products" % marketplace) if not self.debug else []
        # all image quality url crawled
        self.products_mba_image_references_already_crawled = self.get_asin_crawled("mba_%s.products_mba_images" % marketplace) if not self.debug else []
        # all images which are already downloaded to storage
        self.products_images_already_downloaded = self.get_asin_crawled("mba_%s.products_images" % marketplace) if not self.debug else []

        if csv_path != "":
            self.df_search_terms = pd.read_csv(csv_path)

        # does not work currently
        # if self.marketplace == "com":
        #     self.custom_settings.update({
        #         "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=True),
        #     })
        

    def start_requests(self):
        urls_mba = []
        headers = get_random_headers(self.marketplace)
        # case use a csv with search terms
        if not self.df_search_terms.empty:
            for i, df_row in self.df_search_terms.iterrows():
                search_term = df_row["search_term"]
                url_mba = url_creator.main([search_term, self.marketplace, self.pod_product, self.sort])
                url_mba_page = url_mba + "&page=1"#+"&ref=sr_pg_"+str(page_number)
                urls_mba.append(url_mba_page)
        else:
            url_mba = url_creator.main([self.keyword, self.marketplace, self.pod_product, self.sort])
            # send_msg(self.target, "Start scraper {} marketplace {} with {} pages and start page {} and sort {}".format(self.name, self.marketplace, self.pages, self.start_page, self.sort), self.api_key)
            self.cloud_logger.info("Start scraper {} marketplace {} with {} pages and start page {} and sort {}".format(self.name, self.marketplace, self.pages, self.start_page, self.sort))

            # if start_page is other than one, crawler should start from differnt page
            until_page = 401

            if self.pages != 0:
                until_page = self.start_page + self.pages
            for page_number in np.arange(self.start_page, until_page, 1):
                if page_number <= 400:
                    url_mba_page = url_mba + "&page="+str(page_number)#+"&ref=sr_pg_"+str(page_number)
                    urls_mba.append(url_mba_page)
        for i, url_mba in enumerate(urls_mba):
            page = i + self.start_page
            # if self.marketplace == "com": 
            #     url_change_zip_code = "https://www.amazon.com/gp/delivery/ajax/address-change.html"
            #     yield scrapy.http.JsonRequest(url=url_change_zip_code, callback=self.change_zip_code, headers=headers, priority=i, data=self.change_zip_code_post_data,
            #                         errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, 'page': page, "url": url_mba, "headers": headers})
            # else:
            self.crawling_job.count_inc("request_count")
            yield scrapy.Request(url=url_mba, callback=self.parse, headers=headers, priority=i,
                                    errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, 'page': page, "url": url_mba, "headers": headers})

    def get_asin_crawled(self, table_id):
        '''
            Returns a unique list of asins that are already crawled
        '''
        bq_client = bigquery.Client(project='mba-pipeline')
        # todo: change table name in future | 
        try:
            list_asin = bq_client.query("SELECT asin FROM " + table_id + " group by asin").to_dataframe().drop_duplicates(["asin"])["asin"].tolist()
        except Exception as e:
            print(str(e))
            list_asin = []
        return list_asin

    def save_content(self, response, url):
        filename = "data/" + self.name + "/content/%s.html" % url.replace("/","_")
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

    def store_df(self):
        filename = "data/" + self.name + "/products_%s.csv" % datetime.datetime.now().date()
        self.df_products.to_csv(filename, index=False)
        filename = "data/" + self.name + "/mba_images_%s.csv" % datetime.datetime.now().date()
        self.df_mba_images.to_csv(filename, index=False)
        filename = "data/" + self.name + "/mba_relevance%s.csv" % datetime.datetime.now().date()
        self.df_mba_relevance.to_csv(filename, index=False)
        self.log('Saved file %s' % filename)

    def is_captcha_required(self, response):
        return "captcha" in response.body.decode("utf-8").lower()

    def get_zip_code_location(self, response):
        try:
            return response.css('span#glow-ingress-line2::text').get().strip()
        except:
            return "unkown"

    def get_count_results(self, response):
        try:
            count_results_bar_text = response.css('span.celwidget div.a-section span::text')[0].get()
            return int(count_results_bar_text.split(" results")[0].split(" ")[-1].replace(',',''))
        except:
            return "unkown"

    def should_zip_code_be_changed(self, response):
        if self.marketplace == "com":
            #zip_code_location = self.get_zip_code_location(response)
            zip_code_location = "unkown"
            if zip_code_location == "unkown":
                count_results = self.get_count_results(response)
                if type(count_results) == int and count_results < 50000:
                    return True
                else:
                    print("Count shirts overview unkown or to small")
                    try:
                        print(response.url, response.meta["proxy"])
                    except:
                        pass
                    return False
            else:
                if zip_code_location.lower() in ["germany"]:
                    test = 0
        else:
            return False

    def change_zip_code(self, response):
        proxy = self.get_proxy(response)
        if self.is_perfect_privacy_proxy(response):
            proxy = response.meta["proxy"]
        print(proxy)
        meta_dict = response.meta
        meta_dict.update({"proxy": proxy, "_rotating_proxy": False})
        self.crawling_job.count_inc("request_count")
        yield response.follow(url=response.meta["url"], callback=self.parse, headers=response.meta["headers"], priority=0,
                                    errback=self.errback_httpbin, meta=meta_dict, dont_filter=True)
        test = 0

    def parse(self, response):
        proxy = self.get_proxy(response)
        url = response.url
        page = response.meta["page"]
        mba_image_item_list = []

        #self.get_zip_code_location(response)
        #self.get_count_results(response)

        if self.is_captcha_required(response):
            self.crawling_job.count_inc("response_captcha_count")
            #self.response_is_ban(request, response, is_ban=True)
            print("Captcha required for proxy: " + proxy)
            self.captcha_count = self.captcha_count + 1
            self.update_ban_count(proxy)
            headers = get_random_headers(self.marketplace)
            # send new request with high priority
            request = scrapy.Request(url=response.meta["url"], callback=self.parse, headers=headers, priority=0, dont_filter=True,
                                    errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, "page": page, "url": response.meta["url"]})
            self.crawling_job.count_inc("request_count")
            yield request
        else:
            
            if self.should_zip_code_be_changed(response):
                print("Proxy does not get all .com results: " + proxy)
                self.update_ban_count(proxy)   
                headers = get_random_headers(self.marketplace)
                # send new request with high priority
                request = scrapy.Request(url=url, callback=self.parse, headers=headers, priority=0, dont_filter=True,
                                        errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, "page": page})
                self.crawling_job.count_inc("request_count")
                yield request
            else:
                self.crawling_job.count_inc("response_successful_count")
                self.ip_addresses.append(response.ip_address.compressed)

                bq_mba_overview_product_list: List[BQMBAOverviewProduct] = self.get_BQMBAOverviewProduct_list(response)
                bq_mba_products_mba_images_list: List[BQMBAProductsMBAImages] = self.get_BQMBAProductsMBAImages_list(response)
                bq_mba_products_mba_relevance_list: List[BQMBAProductsMBARelevance] = self.get_BQMBAProductsMBARelevance_list(response, page)

                # store product data in bq
                for bq_mba_overview_product in bq_mba_overview_product_list:
                    if bq_mba_overview_product.asin not in self.products_already_crawled:
                        yield bq_mba_overview_product
                        self.products_already_crawled.append(bq_mba_overview_product.asin)

                    # crawl only image if not already crawled
                    if bq_mba_overview_product.asin not in self.products_images_already_downloaded:
                        mba_image_item_list.append(MBAImageItem(url=bq_mba_overview_product.url_image_hq, asin=bq_mba_overview_product.asin, url_lowq=bq_mba_overview_product.url_image_lowq))
                        self.products_images_already_downloaded.append(bq_mba_overview_product.asin)

                # store products_mba_images in BQ
                for bq_mba_products_mba_images in bq_mba_products_mba_images_list:
                    if bq_mba_products_mba_images.asin not in self.products_mba_image_references_already_crawled:
                        yield bq_mba_products_mba_images
                        self.products_mba_image_references_already_crawled.append(bq_mba_products_mba_images.asin)

                # store products_mba_relevance in BQ
                for bq_mba_products_mba_relevance in bq_mba_products_mba_relevance_list:
                    yield bq_mba_products_mba_relevance

                # crawl images
                mba_image_items = MBAImageItems(marketplace=self.marketplace, fs_product_data_col_path=self.fs_product_data_col_path, image_items=mba_image_item_list)
                if self.debug:
                    mba_image_items.image_items = mba_image_items.image_items[0:2]
                if self.marketplace in ["com", "de"]:
                    yield mba_image_items
                
                self.page_count = self.page_count + 1


