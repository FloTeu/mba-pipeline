import scrapy
import datetime
from pathlib import Path
#from proxy import proxy_handler
import pandas as pd
from typing import List
from scrapy.exceptions import CloseSpider

import time
import traceback

# from scrapy.contrib.spidermiddleware.httperror import HttpError
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, ConnectionLost
from twisted.web._newclient import ResponseNeverReceived
from scrapy.core.downloader.handlers.http11 import TunnelError

from mwfunctions.logger import get_logger
from mwfunctions import environment
from mwfunctions.crawler.proxy import proxy_handler
from mwfunctions.crawler.proxy.utils import get_random_headers, send_msg
from mwfunctions.crawler.mw_scrapy.spider_base import MBAProductSpider
from mwfunctions.crawler.preprocessing import create_url_csv
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingType, CrawlingInputItem
from mwfunctions.pydantic.bigquery_classes import BQMBAProductsDetails, BQMBAProductsDetailsDaily, BQMBAProductsNoBsr, BQMBAProductsNoMbaShirt
from mwfunctions.io import str2bool

environment.set_cloud_logging()
LOGGER = get_logger(__name__, labels_dict={"topic": "crawling", "target": "product_page", "type": "scrapy"}, do_cloud_logging=True)



class MBALocalProductSpider(MBAProductSpider):
    name = "mba_product"
    website_crawling_target = CrawlingType.PRODUCT.value
    Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []
    captcha_count = 0
    page_count = 0
    was_banned = {}

    # HINT: should be calles ba settings, since settings will be changed with file string replace
    # custom_settings = {
    #     "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=True),
    # }

    def __init__(self, mba_product_request: CrawlingMBAProductRequest, url_data_path=None, *args, **kwargs):
        super(MBALocalProductSpider, self).__init__(*args, **mba_product_request.dict())
        # TODO: Add functionality to download url data directly within init
        self.daily = str2bool(mba_product_request.daily)
        self.allowed_domains = ['amazon.' + self.marketplace]
        self.mba_product_request = mba_product_request
        self.url_data_path = url_data_path


    def start_requests(self):
        self.reset_was_banned_every_hour()

        # get crawling input from csv file
        if self.url_data_path:
            urls = pd.read_csv(self.url_data_path, engine="python")["url"].tolist()
            asins = pd.read_csv(self.url_data_path)["asin"].tolist()
        # get crawling input from provided asins
        elif self.mba_product_request.asins_to_crawl:
            asins = self.mba_product_request.asins_to_crawl
            urls = [CrawlingInputItem(asin=asin, marketplace=self.marketplace).url for asin in asins]
        # get crawling input from BQ
        else:
            crawling_input_items: List[create_url_csv.CrawlingInputItem] = create_url_csv.get_crawling_input_items(self.mba_product_request, bq_project_id=self.bq_project_id)
            urls = [crawling_input_item.url for crawling_input_item in crawling_input_items]
            asins = [crawling_input_item.asin for crawling_input_item in crawling_input_items]

        # send_msg(self.target, "Start scraper {} daily {} with {} products".format(self.name, self.daily, len(urls)), self.api_key)
        LOGGER.info("Start scraper {} daily {} with {} products".format(self.name, self.daily, len(urls)))
        print("Start scraper {} daily {} with {} products".format(self.name, self.daily, len(urls)))
        self.crawling_job.number_of_target_pages = len(urls)

        for url, asin in zip(urls, asins):
            #proxies = proxy_handler.get_random_proxy_url_dict()
            headers = get_random_headers(self.marketplace)
            self.crawling_job.count_inc("request_count")
            yield scrapy.Request(url=url, callback=self.parse, headers=headers, priority=1,
                                    errback=self.errback_httpbin, meta={"asin": asin, "max_proxies_to_try": 20, "url": url}) # "proxy": proxies["http"],

    def save_content(self, response, asin):
        filename = "data/" + self.name + "/content/%s.html" % asin
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

    def status_update(self):
        if self.page_count % 100 == 0:
            print("Crawled {} pages".format(self.page_count))

    def parse(self, response):
        try:
            asin = response.meta["asin"]
            proxy = self.get_proxy(response)

            url = response.url
            if self.is_captcha_required(response):
                self.yield_again_if_captcha_required(url, proxy, asin=asin)
            # do not proceed if its not a mba shirt
            elif not self.is_mba_shirt(response):
                self.crawling_job.count_inc("response_successful_count")
                yield BQMBAProductsNoMbaShirt(asin=asin, url=url)
            else:
                self.crawling_job.count_inc("response_successful_count")
                self.ip_addresses.append(response.ip_address.compressed)

                if self.daily:
                    bq_mba_products_details_daily: BQMBAProductsDetailsDaily = self.get_BQMBAProductsDetailsDaily(response, asin)
                    yield bq_mba_products_details_daily
                else:
                    bq_mba_products_details: BQMBAProductsDetails = self.get_BQMBAProductsDetails(response, asin)
                    yield bq_mba_products_details

                self.page_count = self.page_count + 1

                self.status_update()

        except Exception as e:
            self.crawling_job.finished_with_error = True
            self.crawling_job.error_msg = str(e)
