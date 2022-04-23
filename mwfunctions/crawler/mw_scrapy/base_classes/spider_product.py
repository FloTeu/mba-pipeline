import threading
from typing import List

from mwfunctions.crawler.mw_scrapy.base_classes.spider_base import MBASpider
from mwfunctions.crawler.mw_scrapy.scrapy_selectors import product as product_selector
from mwfunctions.pydantic.bigquery_classes import BQMBAProductsNoBsr, BQMBAProductsDetails, BQMBAProductsDetailsDaily
from mwfunctions.pydantic.crawling_classes import CrawlingInputItem
from mwfunctions.time import get_berlin_timestamp


class MBAProductSpider(MBASpider):

    def __init__(self, *args, **kwargs):
        super(MBAProductSpider, self).__init__(*args, **kwargs)

        # list of BQ table rows which will be inserted to BQ at closing spider event
        self.no_bsr_products: List[BQMBAProductsNoBsr] = []

    def is_mba_shirt(self, response):
        # mba shirts have always fit type (Herren, Damen, Kinder)
        # TODO: inline-twister-row-fit_type can also exist. New Designs of amazon. Use simple fit_type is in html maybe??
        return response.css('div#centerCol').get().count("fit_type") > 1
        # return len(response.css('div#variation_fit_type span')) > 0

    def reset_was_banned_every_hour(self, reset_time_sec=60 * 60):
        # TODO: This function prevents fastapi from sending response, because proces.start() never finsihses
        # reset proxies every hour
        self.reset_ban = threading.Timer(reset_time_sec, self.reset_was_banned_every_hour)
        self.reset_ban.start()
        self.was_banned = {}

    def is_daily_crawl(self, response):
        """ Returns if crawl is daily crawl (which means it is crawled not the first time but regularly for n time) or first time crawl
            If daily is contained in meta use it. Otherwise take the attr daily of object
        """
        return response.meta.get("daily", self.daily if hasattr(self, "daily") else False)

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

    # def yield_BQMBAProductsNoBsr(self, asin):
    #     cw_input = CrawlingInputItem(marketplace=self.marketplace, asin=asin)
    #     # Hint: if yield is in exception statement, function returns always a generator
    #     yield BQMBAProductsNoBsr(asin=asin, url=cw_input.url)

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
            if self.is_daily_crawl(response):
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

    @staticmethod
    def contains_product_detail_information(response):
        return product_selector.get_product_information_lis(response) != []

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
                                    mba_bsr_str=mba_bsr_str,mba_bsr=mba_bsr, mba_bsr_categorie=str(mba_bsr_categorie), timestamp=get_berlin_timestamp(without_tzinfo=True))

    def get_BQMBAProductsDetailsDaily(self, response, asin) -> BQMBAProductsDetailsDaily:
        mba_bsr_str, mba_bsr, array_bsr, mba_bsr_categorie = self.get_product_bsr(response, asin)
        customer_review_score_mean, customer_review_score, customer_review_count= self.get_product_customer_review(response)
        price_str, price = self.get_product_price(response)
        return BQMBAProductsDetailsDaily(asin=asin, price=price, price_str=price_str, bsr=mba_bsr,
                                         bsr_str=mba_bsr_str, array_bsr=str(array_bsr), array_bsr_categorie=str(mba_bsr_categorie),
                                         customer_review_score_mean=customer_review_score_mean, customer_review_score=customer_review_score_mean,
                                         customer_review_count=customer_review_count, timestamp=get_berlin_timestamp(without_tzinfo=True))