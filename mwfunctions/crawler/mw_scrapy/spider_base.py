import scrapy
import traceback
import pandas as pd
import time
import threading

from typing import List
from datetime import date, datetime

from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, ConnectionLost
from twisted.web._newclient import ResponseNeverReceived
from scrapy.core.downloader.handlers.http11 import TunnelError

from mwfunctions.crawler.proxy import proxy_handler
from mwfunctions.pydantic.crawling_classes import CrawlingInputItem, CrawlingType
from mwfunctions.pydantic.bigquery_classes import BQMBAOverviewProduct, BQMBAProductsMbaImages, BQMBAProductsMbaRelevance, BQMBAProductsDetails, BQMBAProductsDetailsDaily, BQMBAProductsNoBsr
import mwfunctions.crawler.mw_scrapy.scrapy_selectors.overview as overview_selector
import mwfunctions.crawler.mw_scrapy.scrapy_selectors.product as product_selector
from mwfunctions.crawler.proxy.utils import get_random_headers
from mwfunctions.environment import is_debug, get_gcp_project
import mwfunctions.cloud.firestore as firestore_fns

from mwfunctions.logger import get_logger
from mwfunctions import environment
from mwfunctions.time import get_berlin_timestamp

MBA_PRODUCT_TYPE2GCS_DIR = {
    "shirt": "mba-shirts"
}

class MBASpider(scrapy.Spider):

    custom_settings = {
        # Set by settings.py
        # "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=False),

        'ITEM_PIPELINES': {
            'mba_crawler.pipelines.MbaCrawlerItemPipeline': 100,
            'mba_crawler.pipelines.MbaCrawlerImagePipeline': 200
        },
    }

    def __init__(self, marketplace, mba_product_type="shirt", debug=True, *args, **kwargs):
        """
            mba_product_type:   Which mba product type should be crawled can be 'shirt' or in future hoodies, tank tops etc.
                                Value decides where to store images in cloud storage
        """
        environment.set_cloud_logging()
        self.cloud_logger = get_logger(__name__, labels_dict={"topic": "crawling", "target": self.website_crawling_target, "type": "scrapy"},
                            do_cloud_logging=True)

        assert mba_product_type in MBA_PRODUCT_TYPE2GCS_DIR, f"mba_product_type '{mba_product_type}' not defined."
        self.marketplace = marketplace
        self.debug = debug
        self.was_banned = {}

        self.custom_settings.update({
            'IMAGES_STORE': f'gs://5c0ae2727a254b608a4ee55a15a05fb7{"-debug" if self.debug or get_gcp_project() == "merchwatch-dev" else ""}/{MBA_PRODUCT_TYPE2GCS_DIR[mba_product_type]}/',
            'GCS_PROJECT_ID': 'mba-pipeline' # google project of storage
            })


        # Change proxy list depending on marketplace and debug and target
        if self.debug:
            # use only private proxies for debugging
            self.custom_settings.update({
                "ROTATING_PROXY_LIST": proxy_handler.get_private_http_proxy_list(self.marketplace == "com"),
            })
        else:
            self.custom_settings.update({
                "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=self.marketplace == "com" and self.website_crawling_target == "overview"),
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
                    if self.website_crawling_target == CrawlingType.PRODUCT.value:
                        if self.daily:
                            yield BQMBAProductsDetailsDaily(asin=response.meta["asin"], price=404.0, price_str="404", bsr=404, bsr_str="404", array_bsr="[404]", array_bsr_categorie="['404']", customer_review_score_mean=404.0, customer_review_score="404", customer_review_count=404)
                        else:
                            yield BQMBAProductsDetails(asin=response.meta["asin"], title="404", brand="404", url_brand="404", price="404", fit_types="[404]", color_names="[404]", color_count=404, product_features='["4040"]', description="404", weight="404", upload_date_str="1995-01-01", upload_date=datetime(1995,1,1),customer_review_score="404", customer_review_count=404, mba_bsr_str="404", mba_bsr='["404"]', mba_bsr_categorie='["404"]')

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
        captcha = "captcha" in response.body.decode("utf-8").lower()
        content_protection = "benningtonschools" in response.body.decode("utf-8").lower() or "shield.ericomcloud" in response.url
        if content_protection:
            print("Found content protection of benningtonschools.org or shield.ericomcloud")
        return content_protection or captcha

    def yield_again_if_captcha_required(self, url, proxy, asin=None):
        self.crawling_job.count_inc("response_captcha_count")
        # self.response_is_ban(request, response, is_ban=True)
        print("Captcha required for proxy: " + proxy)
        self.captcha_count = self.captcha_count + 1
        self.update_ban_count(proxy)
        headers = get_random_headers(self.marketplace)
        # send new request with high priority
        request = scrapy.Request(url=url, callback=self.parse, headers=headers, priority=0,
                                 dont_filter=True,
                                 errback=self.errback_httpbin, meta={"asin": asin, "url": url})
        self.crawling_job.count_inc("request_count")
        yield request

    def save_content(self, response, file_name):
        filename = "data/" + self.name + "/content/%s.html" % file_name
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

    def closed(self, reason):
        # save crawling job in firestore
        print("Save crawling job to Firestore")
        self.crawling_job.end_timestamp = get_berlin_timestamp(without_tzinfo=True)
        self.crawling_job.set_duration_in_min()
        firestore_fns.write_document_dict(self.crawling_job.dict(),f"{self.fs_log_col_path}/{self.crawling_job.id}")


class MBAOverviewSpider(MBASpider):

    def __init__(self, sort, pages=0, start_page=1, keyword="", *args, **kwargs):
        super(MBAOverviewSpider, self).__init__(*args, **kwargs)

        self.sort = sort # e.g. newest
        self.keyword = keyword
        self.pages = int(pages)
        self.start_page = int(start_page)

    '''
    ### Selector wrappers
    '''
    def get_overview_price(self, overview_response_product):
        try:
            return overview_selector.mba_get_price(overview_response_product, self.marketplace)
        except Exception as e:
            self.log_error(e, "Could not get price data")
            raise e

    def get_overview_title(self, overview_response_product):
        try:
            return overview_selector.mba_get_title(overview_response_product)
        except Exception as e:
            self.log_error(e, "Could not get title")
            raise e

    def get_overview_brand(self, overview_response_product):
        try:
            return overview_selector.mba_get_brand(overview_response_product)
        except Exception as e:
            # its possible that amazon does not show brand on overview page. Therefore raise is not neccessary.
            self.log_warning(e, "Could not get brand")
            return None

    def get_overview_url_product(self, overview_response_product, response_url):
        try:
            return overview_selector.mba_get_url_product(overview_response_product, response_url)
        except Exception as e:
            self.log_error(e, "Could not get url of product")
            raise e

    def get_overview_image_urls(self, overview_response_product):
        try:
            return overview_selector.mba_get_img_urls(overview_response_product)
        except Exception as e:
            self.log_error(e, "Could not get image urls")
            raise e

    def get_overview_url_image_lowq(self, overview_response_product):
        return self.get_overview_image_urls(overview_response_product)[0]

    def get_overview_url_image_hq(self, overview_response_product):
        return self.get_overview_image_urls(overview_response_product)[-1]

    def get_overview_asin(self, overview_response_product):
        try:
            return overview_selector.mba_get_asin(overview_response_product)
        except Exception as e:
            self.log_error(e, "Could not get asin of product")
            raise e

    def get_overview_uuid(self, overview_response_product):
        try:
            return overview_selector.mba_get_uuid(overview_response_product)
        except Exception as e:
            self.log_error(e, "Could not get uuid of product")
            raise e

    '''
    ### Data class functions
    '''
    def is_shirt(self, overview_response_product):
        try:
            asin = overview_selector.mba_get_asin(overview_response_product)
            return asin not in ["", None]
        except Exception as e:
            return False

    def get_overview_products_response_list(self, overview_response):
        return [overview_response_product for overview_response_product in overview_response.css('div.sg-col-inner') if self.is_shirt(overview_response_product)]

    def get_BQMBAOverviewProduct_list(self, overview_response) -> List[BQMBAOverviewProduct]:
        response_url = overview_response.url
        bq_mba_overview_product_list = []
        for overview_response_product in self.get_overview_products_response_list(overview_response):
            bq_mba_overview_product_list.append(BQMBAOverviewProduct(asin=self.get_overview_asin(overview_response_product), title=self.get_overview_title(overview_response_product),
                                 brand=self.get_overview_brand(overview_response_product), url_product=self.get_overview_url_product(overview_response_product, response_url),
                                 url_image_lowq=self.get_overview_url_image_lowq(overview_response_product), url_image_hq=self.get_overview_url_image_hq(overview_response_product),
                                 price=self.get_overview_price(overview_response_product), uuid=self.get_overview_uuid(overview_response_product)))
        return bq_mba_overview_product_list

    def get_BQMBAProductsMBAImages_list(self, overview_response) -> List[BQMBAProductsMbaImages]:
        bq_mba_products_mba_images_list = []
        for overview_response_product in self.get_overview_products_response_list(overview_response):
            url_image_lowq,url_image_q2,url_image_q3,url_image_q4,url_image_hq = self.get_overview_image_urls(overview_response_product)
            bq_mba_products_mba_images_list.append(BQMBAProductsMbaImages(asin=self.get_overview_asin(overview_response_product), url_image_lowq=url_image_lowq,
                                                                          url_image_q2=url_image_q2, url_image_q3=url_image_q3, url_image_q4=url_image_q4, url_image_hq=url_image_hq
                                                                          ))
        return bq_mba_products_mba_images_list

    def get_BQMBAProductsMBARelevance_list(self, overview_response, page, products_per_page=48) -> List[BQMBAProductsMbaRelevance]:
        bq_mba_products_mba_relevance_list = []
        for shirt_number_page, overview_response_product in enumerate(self.get_overview_products_response_list(overview_response)):
            number = int(shirt_number_page + ((int(page) - 1) * products_per_page))
            bq_mba_products_mba_relevance_list.append(BQMBAProductsMbaRelevance(asin=self.get_overview_asin(overview_response_product), sort=self.sort,
                                                                                number=number))
        return bq_mba_products_mba_relevance_list


class MBAProductSpider(MBASpider):

    def __init__(self, *args, **kwargs):
        super(MBAProductSpider, self).__init__(*args, **kwargs)

        # list of BQ table rows which will be inserted to BQ at closing spider event
        self.no_bsr_products: List[BQMBAProductsNoBsr] = []

    def is_mba_shirt(self, response):
        # mba shirts have always fit type (Herren, Damen, Kinder)
        return len(response.css('div#variation_fit_type span')) > 0

    def reset_was_banned_every_hour(self, reset_time_sec=60 * 60):
        # reset proxies every hour
        self.reset_ban = threading.Timer(reset_time_sec, self.reset_was_banned_every_hour)
        self.reset_ban.start()
        self.was_banned = {}

    '''
    ### Selector wrappers
    '''
    def get_product_price(self, response):
        try:
            return product_selector.get_price(response, self.marketplace)
        except Exception as e:
            self.crawling_job.count_inc("price_not_found_count")
            self.log_warning(e, "Could not get price data")
            return "", 0.0

    def yield_BQMBAProductsNoBsr(self, asin):
        cw_input = CrawlingInputItem(marketplace=self.marketplace, asin=asin)
        # Hint: if yield is in exception statement, function returns always a generator
        yield BQMBAProductsNoBsr(asin=asin, url=cw_input.url)

    def get_product_bsr(self, response, asin):
        try:
            return product_selector.get_bsr(response, self.marketplace)
        except Exception as e:
            self.log_error(e, "Could not get BSR data")
            if "no bsr" in str(e): #  catch error no bsr but review count
                cw_input = CrawlingInputItem(marketplace=self.marketplace, asin=asin)
                self.no_bsr_products.append(BQMBAProductsNoBsr(asin=asin, url=cw_input.url))
                # self.yield_BQMBAProductsNoBsr(asin)
                pass
            if self.daily:
                return "", 0, [], []
            else:
                # Cases exists like https://www.amazon.com/dp/B0855BCBZ6, which should have BSR but dont contain it on html
                # Therefore, we want to crawl it just once (if not daily crawl)
                return "", 0, [], []

    def get_product_customer_review(self, response):
        try:
            return product_selector.get_customer_review(response, self.marketplace)
        except Exception as e:
            self.log_error(e, "Could not get review data")
            raise e

    def get_product_title(self, response):
        try:
            return product_selector.get_title(response)
        except Exception as e:
            self.log_error(e, "Could not get title")
            raise e

    def get_product_brand_infos(self, response):
        try:
            return product_selector.get_brand_infos(response)
        except Exception as e:
            self.log_error(e, "Could not get brand")
            raise e

    def get_product_fit_types(self, response):
        try:
            return product_selector.get_fit_types(response)
        except Exception as e:
            self.log_error(e, "Could not get fit types")
            raise e

    def get_product_color_infos(self, response):
        try:
            return product_selector.get_color_infos(response)
        except Exception as e:
            self.log_error(e, "Could not get colors")
            raise e

    def get_product_product_features(self, response):
        try:
            return product_selector.get_product_features(response)
        except Exception as e:
            self.log_error(e, "Could not get product featurs/listings")
            raise e

    def get_product_description(self, response):
        try:
            return product_selector.get_description(response)
        except Exception as e:
            self.log_warning(e, "Could not get product description")
            return ""

    def get_product_weight(self, response):
        try:
            return product_selector.get_weight(response)
        except Exception as e:
            self.log_warning(e, "Could not get product weight")
            return "not found"

    def get_product_upload_date(self, response):
        try:
            return product_selector.get_upload_date(response)
        except Exception as e:
            self.log_error(e, "Could not get product upload date")
            raise e

    '''
    ### Data class functions
    '''
    def get_BQMBAProductsDetails(self, response, asin) -> BQMBAProductsDetails:
        mba_bsr_str, mba_bsr, array_bsr, mba_bsr_categorie= self.get_product_bsr(response, asin)
        customer_review_score_mean, customer_review_score, customer_review_count= self.get_product_customer_review(response)
        price_str, price = self.get_product_price(response)
        brand, url_brand = self.get_product_brand_infos(response)
        color_names, color_count = self.get_product_color_infos(response)
        upload_date_str, upload_date = self.get_product_upload_date(response)
        return BQMBAProductsDetails(asin=asin, title=self.get_product_title(response),brand=brand,url_brand=url_brand,
                                    price=price_str,fit_types=str(self.get_product_fit_types(response)),color_names=str(color_names), color_count=color_count,
                                    product_features=str(self.get_product_product_features(response)), description=self.get_product_description(response),weight=self.get_product_weight(response),
                                    upload_date=upload_date, upload_date_str=upload_date_str,customer_review_count=customer_review_count, customer_review_score=customer_review_score,
                                    mba_bsr_str=mba_bsr_str,mba_bsr=mba_bsr, mba_bsr_categorie=str(mba_bsr_categorie))

    def get_BQMBAProductsDetailsDaily(self, response, asin) -> BQMBAProductsDetailsDaily:
        mba_bsr_str, mba_bsr, array_bsr, mba_bsr_categorie = self.get_product_bsr(response, asin)
        customer_review_score_mean, customer_review_score, customer_review_count= self.get_product_customer_review(response)
        price_str, price = self.get_product_price(response)
        return BQMBAProductsDetailsDaily(asin=asin, price=price, price_str=price_str, bsr=mba_bsr,
                                         bsr_str=mba_bsr_str, array_bsr=str(array_bsr), array_bsr_categorie=str(mba_bsr_categorie),
                                         customer_review_score_mean=customer_review_score_mean, customer_review_score=customer_review_score_mean,
                                         customer_review_count=customer_review_count)
