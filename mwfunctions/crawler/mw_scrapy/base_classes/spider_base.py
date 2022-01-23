import scrapy
import traceback
import time
import logging

from datetime import datetime
from pathlib import Path

from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, ConnectionLost
from twisted.web._newclient import ResponseNeverReceived
from scrapy.core.downloader.handlers.http11 import TunnelError

from mwfunctions.crawler.proxy import proxy_handler
from mwfunctions.cloud.firestore import get_document_snapshot
from mwfunctions.pydantic.crawling_classes import CrawlingType, CrawlingMBARequest
from mwfunctions.pydantic.security_classes import MWSecuritySettings, EndpointId, EndpointServiceDevOp
from mwfunctions.pydantic.firestore.crawling_log_classes import FSMBACrawlingProductLogs
from mwfunctions.pydantic.firestore.mba_shirt_classes import FSMBAShirt
from mwfunctions.pydantic.bigquery_classes import BQMBAProductsDetails, BQMBAProductsDetailsDaily
from mwfunctions.crawler.proxy.utils import get_random_headers
from mwfunctions.environment import get_gcp_project, set_default_gcp_project_if_not_exists
from mwfunctions.pydantic.firestore.collections import MWRootCollectionType, MWRootCollection
import mwfunctions.cloud.firestore as firestore_fns
from mwfunctions.profiling import get_memory_used_in_gb

from mwfunctions.logger import get_logger
from mwfunctions import environment
from mwfunctions.time import get_england_timestamp

MBA_PRODUCT_TYPE2GCS_DIR = {
    "shirt": "mba-shirts"
}

class MBASpider(scrapy.Spider):


    def __init__(self, mba_crawling_request: CrawlingMBARequest, marketplace, crawling_job_id, security_file_path, mba_product_type="shirt", fs_crawling_log_parent_doc_path=None, request_input_to_log_list=[], debug=True, *args, **kwargs):
        """
            mba_product_type:   Which mba product type should be crawled can be 'shirt' or in future hoodies, tank tops etc.
                                Value decides where to store images in cloud storage
        """
        self.memory_in_gb_start: float = get_memory_used_in_gb()
        environment.set_cloud_logging()
        self.cloud_logger = get_logger(__name__, labels_dict={"topic": "crawling", "target": self.website_crawling_target, "type": "scrapy"},
                            do_cloud_logging=True)

        assert mba_product_type in MBA_PRODUCT_TYPE2GCS_DIR, f"mba_product_type '{mba_product_type}' not defined."
        self.mba_crawling_request = mba_crawling_request
        self.marketplace = marketplace
        self.debug = debug
        self.crawling_job_id=crawling_job_id
        self.fs_crawling_log_parent_doc_path = fs_crawling_log_parent_doc_path
        self.request_input_to_log_list = request_input_to_log_list
        self.was_banned = {}
        self.custom_settings = {}
        crawling_product_logs: FSMBACrawlingProductLogs = FSMBACrawlingProductLogs(marketplace=self.marketplace)
        self.crawling_product_logs_subcol_path = f"{crawling_product_logs.get_fs_doc_path()}/{'overview' if self.website_crawling_target == CrawlingType.REALTIME_RESEARCH.value else self.website_crawling_target}"
        self.crawling_product_logs_image_subcol_path = f"{crawling_product_logs.get_fs_doc_path()}/{CrawlingType.IMAGE}"
        mw_security_settings: MWSecuritySettings = MWSecuritySettings(security_file_path)


        if not self.debug:
            # prevent log everything in cloud run/ and normal logging
            logging.getLogger('scrapy').setLevel(logging.WARNING)

        set_default_gcp_project_if_not_exists()

        if self.website_crawling_target in [CrawlingType.OVERVIEW.value, CrawlingType.REALTIME_RESEARCH.value]:
            if self.debug:
                self.image_pipeline_endpoint_url = mw_security_settings.endpoints[EndpointId.CRAWLER_IMAGE_PIPELINE].devop2url[EndpointServiceDevOp.DEBUG]
            elif get_gcp_project() == "merchwatch-dev":
                self.image_pipeline_endpoint_url = mw_security_settings.endpoints[EndpointId.CRAWLER_IMAGE_PIPELINE].devop2url[EndpointServiceDevOp.DEV]
            else:
                self.image_pipeline_endpoint_url = mw_security_settings.endpoints[EndpointId.CRAWLER_IMAGE_PIPELINE].devop2url[EndpointServiceDevOp.PROD]


        if mba_crawling_request.use_image_crawling_pipeline:
            self.custom_settings.update({
                'ITEM_PIPELINES': {
                    'mba_crawler.pipelines.MbaCrawlerItemPipeline': 100,
                    'mba_crawler.pipelines.MbaCrawlerImagePipeline': 200
                },
                'IMAGES_STORE': f'gs://5c0ae2727a254b608a4ee55a15a05fb7{"-debug" if self.debug or get_gcp_project() == "merchwatch-dev" else ""}/{MBA_PRODUCT_TYPE2GCS_DIR[mba_product_type]}/',
                'GCS_PROJECT_ID': 'mba-pipeline' # google project of storage
                })
        else:
            self.custom_settings.update({
                'ITEM_PIPELINES': {
                    'mba_crawler.pipelines.MbaCrawlerItemPipeline': 100,
                    # 'mba_crawler.pipelines.MbaCrawlerImagePipeline': 200
                },
            })

        # Change proxy list depending on marketplace and debug and target
        if self.debug or self.website_crawling_target == CrawlingType.IMAGE.value:
            # use only private proxies for debugging
            self.custom_settings.update({
                "ROTATING_PROXY_LIST": proxy_handler.get_private_http_proxy_list(mw_security_settings, self.marketplace == "com"),
            })
        else:
            self.custom_settings.update({
                "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(mw_security_settings, only_usa=self.marketplace == "com"),# and self.website_crawling_target == "overview"),
                #"ROTATING_PROXY_LIST": proxy_handler.get_private_http_proxy_list(only_usa=self.marketplace == "com" and self.website_crawling_target == "overview"),
            })
        super().__init__(**kwargs)  # python3


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
            Code to add custom settings AFTER init of spider classes
        """
        # init scrapy spider classes (child of MBASpider)
        spider = cls(*args, **kwargs)

        # Update setting with custom_settings (unfreeze it first to make it changable)
        crawler.settings.frozen = False
        crawler.settings.update(spider.custom_settings)
        crawler.settings.freeze()

        # set crawler to spider
        spider._set_crawler(crawler)
        return spider

    def log_error(self, e, custom_msg):
        self.cloud_logger.error(f"{custom_msg}. \nError message: {e}. \nTraceback {traceback.format_exc()}")

    def log_warning(self, e, custom_msg):
        self.crawling_job.count_inc("warning_count")
        self.cloud_logger.warning(f"{custom_msg}. \nError message: {e}. \nTraceback {traceback.format_exc()}")

    def errback_httpbin(self, failure):
        # log all errback failures,
        # you may need the failure's type
        self.logger.error(repr(failure))

        # if isinstance(failure.value, HttpError):
        if failure.check(HttpError):
            # you can get the response
            response = failure.value.response
            try:
                if response.status >= 500 and response.status < 600:
                    self.crawling_job.count_inc("response_5XX_count")
                if response.status >= 300 and response.status < 400:
                    self.crawling_job.count_inc("response_3XX_count")

                # if 404 update big query
                if response.status == 404:
                    self.crawling_job.count_inc("response_404_count")
                    # TODO: if product is the target yield BQ item with 404 data
                    # update FS data directly
                    fs_doc_snap = get_document_snapshot(
                        f"{MWRootCollection(self.marketplace, MWRootCollectionType.SHIRTS)}/{response.meta['asin']}")
                    if fs_doc_snap.exists:
                        fs_doc = FSMBAShirt.parse_fs_doc_snapshot(fs_doc_snap)
                        fs_doc.update_takedown().write_to_firestore(exclude_doc_id=True,overwrite_doc=False)

                    if self.website_crawling_target in [CrawlingType.PRODUCT.value, CrawlingType.REALTIME_RESEARCH.value] and "asin" in response.meta:
                        if self.is_daily_crawl(response):
                            yield {"pydantic_class": BQMBAProductsDetailsDaily(asin=response.meta["asin"], price=404.0, price_str="404", bsr=404, bsr_str="404", array_bsr="[404]", array_bsr_categorie="['404']", customer_review_score_mean=404.0, customer_review_score="404", customer_review_count=404)}
                        else:
                            yield {"pydantic_class": BQMBAProductsDetails(asin=response.meta["asin"], title="404", brand="404", url_brand="404", price="404", fit_types="[404]", color_names="[404]", color_count=404, product_features='["4040"]', description="404", weight="404", upload_date_str="1995-01-01", upload_date=datetime(1995,1,1),customer_review_score="404", customer_review_count=404, mba_bsr_str="404", mba_bsr='["404"]', mba_bsr_categorie='["404"]')}

                        print("HttpError on asin: {} | status_code: {} | ip address: {}".format(response.meta["asin"],
                                                                                                response.status,
                                                                                                response.ip_address.compressed))
                else:
                    print("HttpError on asin: {} | status_code: {} | ip address: {}".format(response.meta["asin"],
                                                                                            response.status,
                                                                                            response.ip_address.compressed))
                    proxy = self.get_proxy(response)
                    self.update_ban_count(proxy)
            except:
                pass
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            proxy = self.get_proxy(request)
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError):
            request = failure.request
            proxy = self.get_proxy(request)
            self.logger.error('TimeoutError on %s', request.url)

        elif failure.check(TCPTimedOutError):
            request = failure.request
            proxy = self.get_proxy(request)
            self.logger.error('TCPTimeoutError on %s', request.url)

        elif failure.check(TunnelError):
            request = failure.request
            proxy = self.get_proxy(request)
            self.logger.error('TunnelError on %s', request.url)

    def get_ban_count(self, proxy):
        ban_count = 0
        if proxy in self.was_banned:
            ban_count = self.was_banned[proxy][0]
        return ban_count

    def get_ban_timestamp(self, proxy):
        ban_timestamp = None
        if proxy in self.was_banned:
            ban_timestamp = self.was_banned[proxy][1]
        return ban_timestamp

    def update_ban_count(self, proxy):
        if proxy in self.was_banned:
            self.was_banned[proxy] = [self.get_ban_count(proxy) + 1, datetime.now()]
        else:
            self.was_banned.update({proxy: [1, datetime.now()]})
        self.crawling_job.count_inc("proxy_ban_count")

    def was_already_banned(self, proxy):
        was_already_banned = False
        # should be banned if captcha was found and it was found in the last 5 minutes for private proxies and 10 minutes for public proxies
        if "perfect-privacy" in proxy:
            if self.get_ban_timestamp(proxy) != None and ((datetime.now() - self.get_ban_timestamp(proxy)).total_seconds() < (60*5)):
                was_already_banned = True
        else:
            if self.get_ban_timestamp(proxy) != None and ((datetime.now() - self.get_ban_timestamp(proxy)).total_seconds() < (60*10)):
                was_already_banned = True
        return was_already_banned

    def response_is_ban(self, request, response, is_ban=False):
        if "_ban" in request.meta and request.meta["_ban"]:
            is_ban = True
        proxy = self.get_proxy(request)
        is_ban = self.was_already_banned(proxy)
        if response.status in [503, 403, 407, 406]:
            self.update_ban_count(proxy)
            is_ban = True
        if is_ban:
            print("Ban proxy: " + proxy)
        should_be_banned = b'banned' in response.body or is_ban
        return should_be_banned

    def exception_is_ban(self, request, exception):
        if type(exception) in [TimeoutError, TCPTimedOutError, DNSLookupError, TunnelError, ConnectionRefusedError, ConnectionLost, ResponseNeverReceived]:
            return True
        elif type(exception) == CloseSpider:
            print("Spider should be closed. Sleep 3 minutes")
            time.sleep(60*3)
            return None
        else:
            return None

    def get_proxy(self, response):
        proxy = ""
        if "proxy" in response.meta:
            proxy = response.meta["proxy"]
        return proxy

    def is_perfect_privacy_proxy(self, response):
        proxy = self.get_proxy(response)
        if "perfect-privacy" in proxy:
            return True
        return False

    def is_captcha_required(self, response):
        body_text = response.body.decode("utf-8").lower()
        captcha = "captcha" in body_text
        content_protection = "benningtonschools" in body_text or "shield.ericomcloud" in response.url
        if content_protection:
            print("Found content protection of benningtonschools.org or shield.ericomcloud")
        del body_text # set memory free
        return content_protection or captcha

    def get_request_again(self, url, asin=None, meta={}):
        headers = get_random_headers(self.marketplace)
        # send new request with high priority
        request = scrapy.Request(url=url, callback=self.parse, headers=headers, priority=0,
                                 dont_filter=True,
                                 errback=self.errback_httpbin, meta={**meta, "asin": asin, "url": url})
        self.crawling_job.count_inc("request_count")
        return request

    def get_request_again_if_captcha_required(self, url, proxy, asin=None, meta={}):
        self.crawling_job.count_inc("response_captcha_count")
        # self.response_is_ban(request, response, is_ban=True)
        print("Captcha required for proxy: " + proxy)
        self.captcha_count = self.captcha_count + 1
        self.update_ban_count(proxy)
        return self.get_request_again(url, asin=asin, meta=meta)

    def save_content(self, response, file_name):
        Path("data/" + self.name + "/content").mkdir(parents=True, exist_ok=True)
        filename = "data/" + self.name + "/content/%s.html" % file_name
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

    def closed(self, reason):
        try:
            self.reset_ban.cancel()
        except Exception as e:
            #send_msg(self.target, "Could not cancel ban reset function", self.api_key)
            self.cloud_logger.info("Could not cancel ban reset function {}".format(str(e)))

        # save crawling job in firestore
        print("Save crawling job to Firestore")
        # self.crawling_job.end_timestamp = get_berlin_timestamp(without_tzinfo=True)
        self.crawling_job.end_timestamp = get_england_timestamp(without_tzinfo=False)
        self.crawling_job.set_duration_in_min()
        if self.crawling_job.memory_log:
            self.crawling_job.memory_log["end"] = get_memory_used_in_gb()
        firestore_fns.write_document_dict(self.crawling_job.dict(),f"{self.fs_log_col_path}/{self.crawling_job.id}", overwrite_doc=True)


