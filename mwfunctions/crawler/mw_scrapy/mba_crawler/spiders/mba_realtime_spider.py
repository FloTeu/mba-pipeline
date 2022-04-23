import copy

import scrapy
import numpy as np
from typing import List
import requests
from contextlib import suppress

from mwfunctions.crawler.proxy.utils import get_random_headers
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_overview_spider import MBAShirtOverviewSpider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_product_general_spider import MBALocalProductSpider
from mwfunctions.crawler.mw_scrapy.base_classes.spider_product import MBAProductSpider
import mwfunctions.crawler.mba.url_creator as url_creator
from mwfunctions.pydantic.crawling_classes import MBAImageItems, MBAImageItem, CrawlingMBAOverviewRequest, CrawlingType, \
    CrawlingMBAImageRequest, CrawlingMBACloudFunctionRequest
from mwfunctions.pydantic.bigquery_classes import BQMBAOverviewProduct, BQMBAProductsMbaImages, BQMBAProductsMbaRelevance, BQMBAProductsNoBsr
from mwfunctions.pydantic.firestore.crawling_log_classes import FSMBACrawlingProductLogsSubcollectionDoc
from mwfunctions.cloud.auth import get_headers_by_service_url
from mwfunctions.cloud.firestore import does_document_exists
from mwfunctions.cloud.firestore import firestore_fns
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingType, CrawlingInputItem, MemoryLog
from mwfunctions.pydantic.firestore import FSDocument

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ValueError("Provided argument is not a bool")

class MBAShirtRealtimeResearchSpider(MBAShirtOverviewSpider, MBALocalProductSpider):
    name = "mba_realtime_research"
    website_crawling_target = CrawlingType.REALTIME_RESEARCH.value
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

    def __init__(self, mba_overview_request: CrawlingMBAOverviewRequest, csv_path="", *args, **kwargs):
        super_attrs = {"mba_crawling_request": mba_overview_request, **mba_overview_request.dict()}
        # TODO init only overview parent
        # super(MBAOverviewSpider, self).__init__(*args, **super_attrs)
        MBAShirtOverviewSpider.__init__(self, mba_overview_request, csv_path=csv_path, *args, **kwargs)
        # Note: since only overview spider is initializied we init attributes of product crawler manually
        self.no_bsr_products: List[BQMBAProductsNoBsr] = []
        self.asin_in_niche_pop_list = []
        self.asin_in_niche_crawled_pop_list = []

    #     # TODO: is pod_product necessary, since we have a class which should crawl only shirts?
    #     # Note: Class could also be extended to crawl more than just shirts..
    #     self.allowed_domains = ['amazon.' + self.marketplace]
    #
    # # called after start_spider of item pipeline
    # def start_requests(self):
    #     self.crawling_job.memory_log = MemoryLog(start=self.memory_in_gb_start)
    #     start_requests_list: List[scrapy.Request] = self.get_start_requests()
    #     for i, sc_request in enumerate(start_requests_list):
    #         self.crawling_job.count_inc("request_count")
    #         yield sc_request

    def write_asin_lists_to_crawling_job(self):
        fs_doc_path = '/'.join(self.fs_log_col_path.split('/')[0:2]) if self.fs_crawling_log_parent_doc_path else f"{self.fs_log_col_path}/{self.crawling_job.id}"
        fs_update_dict = {}
        if len(self.asin_in_niche_pop_list) > 0:
            fs_update_dict.update({"asins_in_niche_list": list(set(self.asin_in_niche_pop_list))})
        if len(self.asin_in_niche_crawled_pop_list) > 0:
            fs_update_dict.update({"asins_crawled_list": list(set(self.asin_in_niche_crawled_pop_list))})
        if fs_update_dict:
            firestore_fns.write_document_dict(fs_update_dict, fs_doc_path, overwrite_doc=False, array_union=True)
            # pop values in list
            self.asin_in_niche_pop_list = []
            self.asin_in_niche_crawled_pop_list = []

    def parse(self, response):
        # if response comes from proxy ban internally, we route parsng method to product page parses if it was a product request
        if "asin" in response.meta:
            for yieldable_obj in self.parse_product_page(response):
                yield yieldable_obj
            # skip Try of parsing of overview page
            return True
        # first try to crawl products pages before parsing overview page
        if not self.is_captcha_required(response) and not self.should_zip_code_be_changed(response):
            self.cur_response = response
            bq_mba_overview_product_list: List[BQMBAOverviewProduct] = self.get_BQMBAOverviewProduct_list(response)
            bq_mba_products_mba_images_list: List[BQMBAProductsMbaImages] = self.get_BQMBAProductsMBAImages_list(response)

            self.asin_in_niche_pop_list.extend([bq_mba_overview_product.asin for bq_mba_overview_product in bq_mba_overview_product_list])
            # reduce writes and write it after first 12 crawlings
            #self.write_asin_lists_to_crawling_job()

            for i, bq_mba_overview_product in enumerate(bq_mba_overview_product_list):
                overview_data_dict = self.get_overview_data_dict(bq_mba_overview_product,
                                                                 bq_mba_products_mba_images_list)

                yield self.request_product_page(bq_mba_overview_product.asin, response, i, overview_data_dict, bq_mba_overview_product.url_product)
        # parse overview page (update BQ and FS data + crawling new images)
        for yieldable_obj in MBAShirtOverviewSpider.parse(self, response):
            yield yieldable_obj

        test = 0

    def get_overview_data_dict(self, bq_mba_overview_product, bq_mba_products_mba_images_list) -> dict:
        price_overview = None
        with suppress(ValueError):
            price_overview = float(bq_mba_overview_product.price.replace(",", "."))
        overview_data_dict = {"price_overview": price_overview}
        try:
            bq_mba_products_mba_images = [bq_mba_products_mba_images for bq_mba_products_mba_images in
                                          bq_mba_products_mba_images_list if
                                          bq_mba_products_mba_images.asin == bq_mba_overview_product.asin][0]

            overview_data_dict = {**overview_data_dict, **bq_mba_products_mba_images.dict()}
        except Exception as e:
            self.log_warning(e, "Could not find mba image data, even if they should exist")
        return overview_data_dict

    def request_product_page(self, asin, response: scrapy.http.Response, nr_product, overview_data_dict, url_product) -> scrapy.Request:
        page_nr = (self.shirts_per_page * (response.meta["page"]-1)) + nr_product
        was_crawled_already = asin not in self.products_first_time_crawled
        headers = response.request.headers
        proxy_meta={}
        if self.get_proxy(response) != "":
            proxy_meta = {
            "proxy": self.get_proxy(response)  # crawl product pages with same proxy as overview crawler
            }

        return scrapy.Request(f"https://www.amazon.{self.marketplace}/dp/{asin}" if f"https://www.amazon.{self.marketplace}" not in url_product else url_product,
                          callback=self.parse_product_page,
                          headers=headers,
                          priority=page_nr,
                          meta={"asin": asin,
                                "total_page_target": self.shirts_per_page*self.pages,
                                "page_nr": page_nr,
                                "daily": was_crawled_already,
                                **proxy_meta,
                                **overview_data_dict})

    def parse_product_page(self, response):
        for yieldable_obj in MBALocalProductSpider.parse(self, response):
            yield yieldable_obj
            item = copy.deepcopy(yieldable_obj)
            if type(item) == dict and "pydantic_class" in item:
                item = item["pydantic_class"]
            if isinstance(item, FSDocument):
                self.asin_in_niche_crawled_pop_list.append(item.asin)
            # write every 12th asin list to FS
            if len(self.asin_in_niche_crawled_pop_list) >= 12:
                # extend asin_list of crawling job.
                self.write_asin_lists_to_crawling_job()



