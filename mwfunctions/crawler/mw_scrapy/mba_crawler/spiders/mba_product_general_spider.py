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
from mwfunctions.pydantic.bigquery_classes import BQMBAProductsDetails, BQMBAProductsDetailsDaily, BQMBAProductsNoBsr, BQMBAProductsNoMbaShirt, get_product_listings_by_list_str
from mwfunctions.pydantic.firestore.mba_shirt_classes import FSMBAShirt, get_bsr_category, FSWatchItemSubCollectionPlotData
from mwfunctions.pydantic.firestore.firestore_classes import GetFSDocsSettings
from mwfunctions.io import str2bool
from mwfunctions.crawler.mw_scrapy.utils import get_urls_asins_for_product_crawling
from mwfunctions.cloud.firestore import get_document_snapshot, OrderByDirection
from mwfunctions.pydantic.firestore.collections import MWRootCollectionType, MWRootCollection

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

        for i, (url, asin) in enumerate(zip(urls, asins)):
            #proxies = proxy_handler.get_random_proxy_url_dict()
            headers = get_random_headers(self.marketplace)
            self.crawling_job.count_inc("request_count")
            yield scrapy.Request(url=url, callback=self.parse, headers=headers, priority=i,
                                    errback=self.errback_httpbin, meta={"asin": asin, "max_proxies_to_try": 20, "url": url,"page_nr":i, "total_page_target": len(asins)}) # "proxy": proxies["http"],

    def status_update(self):
        if self.page_count % 100 == 0:
            print("Crawled {} pages".format(self.page_count))

    def parse(self, response):
        try:
            asin = response.meta["asin"]
            total_page_target = response.meta["total_page_target"]
            page_nr = response.meta["page_nr"]
            proxy = self.get_proxy(response)
            # # TODO: just testing
            # if page_nr == total_page_target-1:
            #     raise CloseSpider
            url = response.url
            if self.is_captcha_required(response):
                yield self.get_request_again_if_captcha_required(url, proxy, asin=asin, meta={"total_page_target": total_page_target, "page_nr": page_nr})
            # do not proceed if its not a mba shirt
            elif not self.is_mba_shirt(response):
                self.crawling_job.count_inc("response_successful_count")
                yield {"pydantic_class": BQMBAProductsNoMbaShirt(asin=asin, url=url)}
            else:
                self.crawling_job.count_inc("response_successful_count")
                self.ip_addresses.append(response.ip_address.compressed)

                # daily table should always be filled (also in case of first time general product crawling)
                bq_mba_products_details_daily: BQMBAProductsDetailsDaily = self.get_BQMBAProductsDetailsDaily(response, asin)
                bq_mba_products_details: BQMBAProductsDetails = self.get_BQMBAProductsDetails(response, asin)
                # workaround for error Spider must return request, item, or None
                yield {"pydantic_class": bq_mba_products_details_daily}

                if not self.daily:
                    yield {"pydantic_class": bq_mba_products_details}
                # else:
                #     fs_doc_snap = get_document_snapshot(f"{MWRootCollection(self.marketplace, MWRootCollectionType.SHIRTS)}/{asin}")
                #     if fs_doc_snap.exists:
                #         fs_doc = FSMBAShirt.parse_fs_doc_snapshot(fs_doc_snap, read_subcollections=[FSWatchItemSubCollectionPlotData], read_subcollection_docs_settings_dict={FSWatchItemSubCollectionPlotData:GetFSDocsSettings(limit=2, order_by="year", order_by_direction=OrderByDirection.DESC)})
                #         # TODO: Update keyword data (splitted in dsubcategories)
                #         # TODO: keep splitted keyword data in FS. But only if its set, otherwise only keywords_meaningful list
                #         fs_doc.update_data(bsr_last=bq_mba_products_details_daily.bsr, bsr_category=get_bsr_category(bq_mba_products_details_daily.array_bsr_categorie, self.marketplace), price_last=bq_mba_products_details_daily.price, score_last=bq_mba_products_details_daily.customer_review_score_mean, brand=bq_mba_products_details.brand, title=bq_mba_products_details.title, listings=get_product_listings_by_list_str(bq_mba_products_details.product_features, self.marketplace), description=bq_mba_products_details.description)
                #         yield {"pydantic_class": fs_doc}

                self.page_count = self.page_count + 1

                self.status_update()

            # yield no_bsr_products
            while len(self.no_bsr_products) > 0:
                yield {"pydantic_class": self.no_bsr_products.pop(0)}

        except Exception as e:
            self.crawling_job.finished_with_error = True
            self.crawling_job.error_msg = str(e)
            raise e
