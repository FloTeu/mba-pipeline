import scrapy
import numpy as np
from typing import List
import requests
from contextlib import suppress

from mwfunctions.crawler.proxy.utils import get_random_headers
from mwfunctions.crawler.mw_scrapy.base_classes.spider_overview import MBAOverviewSpider
from mwfunctions.crawler.mw_scrapy.base_classes.spider_product import MBAProductSpider
import mwfunctions.crawler.mba.url_creator as url_creator
from mwfunctions.pydantic.crawling_classes import MBAImageItems, MBAImageItem, CrawlingMBAOverviewRequest, CrawlingType, \
    CrawlingMBAImageRequest, CrawlingMBACloudFunctionRequest
from mwfunctions.pydantic.bigquery_classes import BQMBAOverviewProduct, BQMBAProductsMbaImages, BQMBAProductsMbaRelevance
from mwfunctions.pydantic.firestore.crawling_log_classes import FSMBACrawlingProductLogsSubcollectionDoc
from mwfunctions.cloud.auth import get_headers_by_service_url
from mwfunctions.cloud.firestore import does_document_exists

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ValueError("Provided argument is not a bool")

class MBAShirtRealtimeResearchSpider(MBAOverviewSpider):#, MBAProductSpider):
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
        MBAOverviewSpider.__init__(self, *args, **super_attrs)
        # TODO: is pod_product necessary, since we have a class which should crawl only shirts? Class could also be extended to crawl more than just shirts..
        self.pod_product = mba_overview_request.mba_product_type
        self.allowed_domains = ['amazon.' + self.marketplace]

    # called after start_spider of item pipeline
    def start_requests(self):
        # use FS to check if data was already crawled. However lists are maintained during crawling to reduce some reading costs
        self.products_already_crawled = [] # self.get_asin_crawled(f"mba_{self.marketplace}.products") #if not self.debug else []
        # all image quality url crawled
        self.products_mba_image_references_already_crawled = [] # self.get_asin_crawled(f"mba_{self.marketplace}.products_mba_images") #if not self.debug else []
        # all images which are already downloaded to storage
        self.products_images_already_downloaded = [] # self.get_asin_crawled(f"mba_{self.marketplace}.products_images") #if not self.debug else []

        urls_mba = []
        headers = get_random_headers(self.marketplace)

        url_mba = url_creator.main([self.keyword, self.marketplace, self.pod_product, self.sort])
        # send_msg(self.target, "Start scraper {} marketplace {} with {} pages and start page {} and sort {}".format(self.name, self.marketplace, self.pages, self.start_page, self.sort), self.api_key)
        self.cloud_logger.info("Start realtime scraper {} marketplace {} with {} pages and start page {} and sort {}".format(self.name, self.marketplace, self.pages, self.start_page, self.sort))

        # if start_page is other than one, crawler should start from differnt page
        until_page = 401

        if self.pages != 0:
            until_page = self.start_page + self.pages
        for page_number in np.arange(self.start_page, until_page, 1):
            if page_number <= 400:
                url_mba_page = url_mba + "&page="+str(page_number)#+"&ref=sr_pg_"+str(page_number)
                urls_mba.append(url_mba_page)

        self.crawling_job.number_of_target_pages = len(urls_mba)
        for i, url_mba in enumerate(urls_mba):
            page = i + self.start_page
            self.crawling_job.count_inc("request_count")
            yield scrapy.Request(url=url_mba, callback=self.parse, headers=headers, priority=i,
                                    errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, 'page': page, "url": url_mba, "headers": headers})

    def parse(self, response):
        try:
            proxy = self.get_proxy(response)
            url = response.url
            page = response.meta["page"]
            mba_image_item_list = []

            if self.is_captcha_required(response):
                yield self.get_request_again_if_captcha_required(url, proxy, meta={"page": page})
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
                    bq_mba_products_mba_images_list: List[BQMBAProductsMbaImages] = self.get_BQMBAProductsMBAImages_list(response)
                    bq_mba_products_mba_relevance_list: List[BQMBAProductsMbaRelevance] = self.get_BQMBAProductsMBARelevance_list(response, page)

                    asin2was_crawled_bool = {bq_mba_overview_product.asin: does_document_exists(f"{self.crawling_product_logs_subcol_path}/{bq_mba_overview_product.asin}", client=self.fs_client) for bq_mba_overview_product in bq_mba_overview_product_list}

                    # store product data in bq
                    for bq_mba_overview_product in bq_mba_overview_product_list:
                        if bq_mba_overview_product.asin not in self.products_already_crawled and not asin2was_crawled_bool[bq_mba_overview_product.asin]:
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


