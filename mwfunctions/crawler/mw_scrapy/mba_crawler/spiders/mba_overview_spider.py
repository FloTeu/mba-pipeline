import scrapy
import pandas as pd
import numpy as np
from typing import List
import requests
from contextlib import suppress
import sys
sys.path.append("...")
sys.path.append("..")
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\flori\\Dropbox\\Apps\\MBA Pipeline\\merchwatch.de\\privacy files\\mba-pipeline-4de1c9bf6974.json"
#from proxy.utils import get_random_headers, send_msg
#from proxy import proxy_handler
#import mba_url_creator as url_creator

# from scrapy.contrib.spidermiddleware.httperror import HttpError

from mwfunctions.crawler.proxy.utils import get_random_headers
from mwfunctions.crawler.mw_scrapy.base_classes.spider_overview import MBAOverviewSpider
import mwfunctions.crawler.mba.url_creator as url_creator
from mwfunctions.pydantic.crawling_classes import MBAImageItems, MBAImageItem, CrawlingMBAOverviewRequest, CrawlingType, \
    CrawlingMBAImageRequest, CrawlingMBACloudFunctionRequest
from mwfunctions.pydantic.bigquery_classes import BQMBAOverviewProduct, BQMBAProductsMbaImages, BQMBAProductsMbaRelevance
from mwfunctions.pydantic.firestore.crawling_log_classes import FSMBACrawlingProductLogsSubcollectionDoc
from mwfunctions.cloud.auth import get_headers_by_service_url
from mwfunctions.cloud.firestore import does_document_exists
from mwfunctions.profiling import get_memory_used_in_gb
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingType, CrawlingInputItem, MemoryLog
from mwfunctions.pydantic.base_classes import MWBaseModel

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
    website_crawling_target = CrawlingType.OVERVIEW.value
    # Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
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

    def __init__(self, mba_overview_request: CrawlingMBAOverviewRequest, csv_path="", *args, **kwargs):
        super_attrs = {"mba_crawling_request": mba_overview_request, **mba_overview_request.dict()}
        # super(MBAShirtOverviewSpider, self).__init__(*args, **super_attrs)
        MBAOverviewSpider.__init__(self, *args, **super_attrs)
        # TODO: is pod_product necessary, since we have a class which should crawl only shirts? Class could also be extended to crawl more than just shirts..
        self.pod_product = mba_overview_request.mba_product_type
        self.allowed_domains = ['amazon.' + self.marketplace]

        if csv_path != "":
            self.df_search_terms = pd.read_csv(csv_path)

        # does not work currently
        # if self.marketplace == "com":
        #     self.custom_settings.update({
        #         "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=True),
        #     })
        
    # called after start_spider of item pipeline
    def start_requests(self):
        self.crawling_job.memory_log = MemoryLog(start=self.memory_in_gb_start)
        start_requests_list: List[scrapy.Request] = self.get_start_requests()
        for i, sc_request in enumerate(start_requests_list):
            self.crawling_job.count_inc("request_count")
            # starting from 1000 to allow product page crawl to have a higher priority than overview crawl
            sc_request.priority = 1000 + i
            yield sc_request

    def get_asin_crawled(self, table_id):
        '''
            Returns a unique list of asins that are already crawled
        '''
        # todo: change table name in future |
        try:
            list_asin = self.bq_client.query("SELECT asin FROM " + table_id + " group by asin").to_dataframe().drop_duplicates(["asin"])["asin"].tolist()
        except Exception as e:
            print(str(e))
            list_asin = []
        return list_asin

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
        try:
            proxy = self.get_proxy(response)
            url = response.url
            page = response.meta["page"]
            mba_image_item_list = []

            self.cur_response = response
            #self.get_zip_code_location(response)
            #self.get_count_results(response)

            if self.is_captcha_required(response):
                yield self.get_request_again_if_captcha_required(url, proxy, meta={"page": page})
            else:
                if self.should_zip_code_be_changed(response):
                    self.crawling_job.count_inc("request_count")
                    yield self.get_zip_code_change_request(response)
                else:
                    self.crawling_job.count_inc("response_successful_count")
                    self.ip_addresses.append(response.ip_address.compressed)

                    bq_mba_overview_product_list: List[BQMBAOverviewProduct] = self.get_BQMBAOverviewProduct_list(response)
                    bq_mba_products_mba_images_list: List[BQMBAProductsMbaImages] = self.get_BQMBAProductsMBAImages_list(response)
                    bq_mba_products_mba_relevance_list: List[BQMBAProductsMbaRelevance] = self.get_BQMBAProductsMBARelevance_list(response, page)

                    asin2was_crawled_bool = {bq_mba_overview_product.asin: does_document_exists(f"{self.crawling_product_logs_subcol_path}/{bq_mba_overview_product.asin}", client=self.fs_client) for bq_mba_overview_product in bq_mba_overview_product_list}

                    # store product data in bq
                    for bq_mba_overview_product in bq_mba_overview_product_list:
                        if bq_mba_overview_product.asin not in self.products_already_crawled and not asin2was_crawled_bool[bq_mba_overview_product.asin]:
                            self.products_first_time_crawled.append(bq_mba_overview_product.asin)
                            yield {"pydantic_class": bq_mba_overview_product}
                            self.crawling_job.count_inc("new_products_count")
                            self.products_already_crawled.append(bq_mba_overview_product.asin)

                            # TODO: fs product log hapens before bq_mba_products_mba_images gets yielded, which leads to ignoring uncrawled data if process is killed
                            # log that overview page was crawled successfully
                            crawling_product_logs_subcol_doc = FSMBACrawlingProductLogsSubcollectionDoc(doc_id=bq_mba_overview_product.asin)
                            crawling_product_logs_subcol_doc.set_fs_col_path(self.crawling_product_logs_subcol_path)
                            crawling_product_logs_subcol_doc.update_timestamp()
                            yield {"pydantic_class": crawling_product_logs_subcol_doc}
                        else:
                            self.crawling_job.count_inc("already_crawled_products_count")
                        # crawl only image if not already crawled
                        if bq_mba_overview_product.asin not in self.products_images_already_downloaded and not does_document_exists(f"{self.crawling_product_logs_image_subcol_path}/{bq_mba_overview_product.asin}", client=self.fs_client):
                            mba_image_item_list.append(MBAImageItem(url=bq_mba_overview_product.url_image_hq, asin=bq_mba_overview_product.asin, url_lowq=bq_mba_overview_product.url_image_lowq))
                            self.products_images_already_downloaded.append(bq_mba_overview_product.asin)

                    # store products_mba_images in BQ
                    for bq_mba_products_mba_images in bq_mba_products_mba_images_list:
                        if bq_mba_products_mba_images.asin not in self.products_mba_image_references_already_crawled and not asin2was_crawled_bool[bq_mba_products_mba_images.asin]:
                            yield {"pydantic_class": bq_mba_products_mba_images}
                            self.products_mba_image_references_already_crawled.append(bq_mba_products_mba_images.asin)

                    # store products_mba_relevance in BQ
                    for bq_mba_products_mba_relevance in bq_mba_products_mba_relevance_list:
                        yield {"pydantic_class": bq_mba_products_mba_relevance}

                    # crawl images
                    mba_image_items = MBAImageItems(marketplace=self.marketplace, fs_product_data_col_path=self.fs_product_data_col_path, image_items=mba_image_item_list)
                    # if self.debug:
                    #     mba_image_items.image_items = mba_image_items.image_items[0:2]
                    if self.marketplace in ["com", "de"] and len(mba_image_items.image_items) > 0:
                        # either crawl images directly in this instance or use loud functions
                        if self.mba_crawling_request.use_image_crawling_pipeline:
                            yield {"pydantic_class": mba_image_items}
                        else:
                            r = None
                            with suppress(requests.exceptions.ReadTimeout):
                                #store_uri: str = Field(description="gs_url for image location")
                                img_pip_request = CrawlingMBAImageRequest(marketplace=self.marketplace, crawling_job_id=f"{self.crawling_job.id}_{page}",
                                                                        mba_product_type=self.pod_product, mba_image_items=mba_image_items, fs_crawling_log_parent_doc_path=f"{self.fs_log_col_path}/{self.crawling_job.id}")
                                cf_request = CrawlingMBACloudFunctionRequest(crawling_type=CrawlingType.IMAGE, crawling_mba_request=img_pip_request)
                                r = requests.post(self.image_pipeline_endpoint_url, data=cf_request.json(),
                                                  headers=get_headers_by_service_url(self.image_pipeline_endpoint_url), timeout=0.1)
                            if r:
                                assert r.status_code == 200, f"Status code should be 200 but is '{r.status_code}', error: {r.text}"

                        #yield {"pydantic_class": mba_image_items}

                    self.page_count = self.page_count + 1

        except Exception as e:
            self.crawling_job.finished_with_error = True
            self.crawling_job.error_msg = str(e)
            raise e


