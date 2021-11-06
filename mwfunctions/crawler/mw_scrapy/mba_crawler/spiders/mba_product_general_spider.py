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
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingType, CrawlingInputItem
from mwfunctions.pydantic.bigquery_classes import BQMBAProductsDetails, BQMBAProductsDetailsDaily, BQMBAProductsNoBsr, BQMBAProductsNoMbaShirt
from mwfunctions.io import str2bool
from mwfunctions.crawler.mw_scrapy.utils import get_urls_asins_for_product_crawling

environment.set_cloud_logging()
LOGGER = get_logger(__name__, labels_dict={"topic": "crawling", "target": "product_page", "type": "scrapy"}, do_cloud_logging=True)



class MBALocalProductSpider(MBAProductSpider):
    name = "mba_product"
    website_crawling_target = CrawlingType.PRODUCT.value
    # Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
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
        super_attrs = {"mba_crawling_request": mba_product_request, **mba_product_request.dict()}
        super(MBALocalProductSpider, self).__init__(*args, **super_attrs)
        # TODO: Add functionality to download url data directly within init
        self.daily = str2bool(mba_product_request.daily)
        self.allowed_domains = ['amazon.' + self.marketplace]
        self.url_data_path = url_data_path


    def start_requests(self):
        self.reset_was_banned_every_hour()

        urls, asins = get_urls_asins_for_product_crawling(self.mba_crawling_request, self.marketplace, self.bq_project_id, url_data_path=self.url_data_path, debug=self.debug)

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
                yield {"pydantic_class": BQMBAProductsNoMbaShirt(asin=asin, url=url)}
            else:
                self.crawling_job.count_inc("response_successful_count")
                self.ip_addresses.append(response.ip_address.compressed)

                # daily table should always be filled (also in case of first time general product crawling)
                bq_mba_products_details_daily: BQMBAProductsDetailsDaily = self.get_BQMBAProductsDetailsDaily(response, asin)
                # workaround for error Spider must return request, item, or None
                yield {"pydantic_class": bq_mba_products_details_daily}

                if not self.daily:
                    bq_mba_products_details: BQMBAProductsDetails = self.get_BQMBAProductsDetails(response, asin)
                    yield {"pydantic_class": bq_mba_products_details}

                self.page_count = self.page_count + 1

                self.status_update()

            # yield no_bsr_products
            while len(self.no_bsr_products) > 0:
                yield {"pydantic_class": self.no_bsr_products.pop(0)}

        except Exception as e:
            self.crawling_job.finished_with_error = True
            self.crawling_job.error_msg = str(e)
