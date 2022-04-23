from typing import List

import numpy as np
import scrapy
from mwfunctions.crawler.mw_scrapy.base_classes.spider_base import MBASpider
from mwfunctions.crawler.mw_scrapy.scrapy_selectors import overview as overview_selector
from mwfunctions.pydantic.bigquery_classes import BQMBAOverviewProduct, BQMBAProductsMbaImages, BQMBAProductsMbaRelevance
from mwfunctions.pydantic.crawling_classes import PODProduct
from mwfunctions.time import get_berlin_timestamp
from mwfunctions.crawler.proxy.utils import get_random_headers
import mwfunctions.crawler.mba.url_creator as url_creator


class MBAOverviewSpider(MBASpider):

    def __init__(self, sort, pages=0, start_page=1, keyword="", mba_product_type=PODProduct.SHIRT, *args, **kwargs):
        # super(MBAOverviewSpider, self).__init__(*args, **kwargs)
        MBASpider.__init__(self, *args, **kwargs)

        self.sort = sort # e.g. newest
        self.keyword = keyword
        self.pages = int(pages)
        self.start_page = int(start_page)
        self.pod_product = mba_product_type


        # use FS to check if data was already crawled. However lists are maintained during crawling to reduce some reading costs
        self.products_already_crawled = [] # self.get_asin_crawled(f"mba_{self.marketplace}.products") #if not self.debug else []
        # all asins which were found the first time
        self.products_first_time_crawled = []
        # all image quality url crawled
        self.products_mba_image_references_already_crawled = [] # self.get_asin_crawled(f"mba_{self.marketplace}.products_mba_images") #if not self.debug else []
        # all images which are already downloaded to storage
        self.products_images_already_downloaded = [] # self.get_asin_crawled(f"mba_{self.marketplace}.products_images") #if not self.debug else []

    '''
    ### Parent functions
    '''
    @staticmethod
    def get_zip_code_location(response):
        try:
            return response.css('span#glow-ingress-line2::text').get().strip()
        except:
            return "unkown"

    def get_count_results(self, response) -> int: # potentially raises Exception
        return overview_selector.mba_get_number_of_products_in_niche(response, self.marketplace)

    def should_zip_code_be_changed(self, response):
        if self.marketplace == "com":
            #zip_code_location = self.get_zip_code_location(response)
            zip_code_location = "unkown"
            if zip_code_location == "unkown":
                try:
                    count_results = self.get_count_results(response)
                except IndexError:
                    count_results = "unkown"
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

    def get_start_requests(self) -> List[scrapy.Request]:
        assert hasattr(self, "crawling_job"), f"{self} must contain crawling_job as attribute"
        start_requests: List[scrapy.Request] = []
        urls_mba = []
        headers = get_random_headers(self.marketplace)

        # case csv file provided
        if hasattr(self, "df_search_terms") and not self.df_search_terms.empty:
            for i, df_row in self.df_search_terms.iterrows():
                search_term = df_row["search_term"]
                url_mba = url_creator.main([search_term, self.marketplace, self.pod_product, self.sort])
                url_mba_page = url_mba + "&page=1"#+"&ref=sr_pg_"+str(page_number)
                urls_mba.append(url_mba_page)
        else:
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
            start_requests.append(scrapy.Request(url=url_mba, callback=self.parse, headers=headers, priority=i,
                                    errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, 'page': page, "url": url_mba, "headers": headers}))
        return start_requests

    def get_zip_code_change_request(self, response):
        proxy = self.get_proxy(response)
        print("Proxy does not get all .com results: " + proxy)
        self.update_ban_count(proxy)
        headers = get_random_headers(self.marketplace)
        # send new request with high priority
        return scrapy.Request(url=response.url, callback=self.parse, headers=headers, priority=0, dont_filter=True,
                                 errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, "page": response.meta["page"]})

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
            # TODO: How to handle case no image exists and only placeholder image is shown? Image url should be set by product page crawler. Or skip this one and try to get image later?
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
    @staticmethod
    def is_shirt( overview_response_product):
        try:
            asin = overview_selector.mba_get_asin(overview_response_product)
            return asin not in ["", None]
        except Exception as e:
            return False

    @staticmethod
    def get_overview_products_response_list(overview_response):
        # must be static because its also used outside of class. I.e. Main of crawling API
        return [overview_response_product for overview_response_product in overview_response.css('div.sg-col-inner') if MBAOverviewSpider.is_shirt(overview_response_product)]

    def get_BQMBAOverviewProduct_list(self, overview_response) -> List[BQMBAOverviewProduct]:
        response_url = overview_response.url
        bq_mba_overview_product_list = []
        for overview_response_product in self.get_overview_products_response_list(overview_response):
            bq_mba_overview_product_list.append(BQMBAOverviewProduct(asin=self.get_overview_asin(overview_response_product), title=self.get_overview_title(overview_response_product),
                                 brand=self.get_overview_brand(overview_response_product), url_product=self.get_overview_url_product(overview_response_product, response_url),
                                 url_image_lowq=self.get_overview_url_image_lowq(overview_response_product), url_image_hq=self.get_overview_url_image_hq(overview_response_product),
                                 price=self.get_overview_price(overview_response_product), uuid=self.get_overview_uuid(overview_response_product), timestamp=get_berlin_timestamp(without_tzinfo=True)))
        return bq_mba_overview_product_list

    def get_BQMBAProductsMBAImages_list(self, overview_response) -> List[BQMBAProductsMbaImages]:
        bq_mba_products_mba_images_list = []
        for overview_response_product in self.get_overview_products_response_list(overview_response):
            url_image_lowq,url_image_q2,url_image_q3,url_image_q4,url_image_hq = self.get_overview_image_urls(overview_response_product)
            bq_mba_products_mba_images_list.append(BQMBAProductsMbaImages(asin=self.get_overview_asin(overview_response_product), url_image_lowq=url_image_lowq,
                                                                          url_image_q2=url_image_q2, url_image_q3=url_image_q3, url_image_q4=url_image_q4, url_image_hq=url_image_hq
                                                                          , timestamp=get_berlin_timestamp(without_tzinfo=True)
                                                                          ))
        return bq_mba_products_mba_images_list

    def get_BQMBAProductsMBARelevance_list(self, overview_response, page, products_per_page=48) -> List[BQMBAProductsMbaRelevance]:
        bq_mba_products_mba_relevance_list = []
        for shirt_number_page, overview_response_product in enumerate(self.get_overview_products_response_list(overview_response)):
            number = int(shirt_number_page + ((int(page) - 1) * products_per_page))
            bq_mba_products_mba_relevance_list.append(BQMBAProductsMbaRelevance(asin=self.get_overview_asin(overview_response_product), sort=self.sort,
                                                                                number=number, timestamp=get_berlin_timestamp(without_tzinfo=True)))
        return bq_mba_products_mba_relevance_list