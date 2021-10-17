import scrapy
import traceback

from typing import List

from mwfunctions.pydantic.bigquery_classes import BQMBAOverviewProduct, BQMBAProductsMBAImages, BQMBAProductsMBARelevance
import mwfunctions.crawler.scrapy.selectors.overview as overview_selector
import mwfunctions.crawler.scrapy.selectors.product as product_selector

from mwfunctions.logger import get_logger
from mwfunctions import environment


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

        self.custom_settings.update({
            'IMAGES_STORE': f'gs://5c0ae2727a254b608a4ee55a15a05fb7{"-debug" if self.debug else ""}/{MBA_PRODUCT_TYPE2GCS_DIR[mba_product_type]}/',
            'GCS_PROJECT_ID': 'mba-pipeline' # google project of storage
            })

        super().__init__(**kwargs)  # python3


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
            Code to add custom settings AFTER init of spider classes
        """
        # init scrapy spider classes (child of MBASpider)
        spider = cls(*args, **kwargs)

        # TODO: try to change proxy list depending on marketplace crawled
        # Update setting with custom_settings
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

class MBAOverviewSpider(MBASpider):

    def __init__(self, marketplace, sort, *args, **kwargs):
        self.sort = sort # e.g. newest
        super(MBAOverviewSpider, self).__init__(marketplace, *args, **kwargs)

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

    def is_shirt(self, overview_response_product):
        try:
            asin = self.get_overview_asin(overview_response_product)
            return True
        except:
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

    def get_BQMBAProductsMBAImages_list(self, overview_response) -> List[BQMBAProductsMBAImages]:
        bq_mba_products_mba_images_list = []
        for overview_response_product in self.get_overview_products_response_list(overview_response):
            url_image_lowq,url_image_q2,url_image_q3,url_image_q4,url_image_hq = self.get_overview_image_urls(overview_response_product)
            bq_mba_products_mba_images_list.append(BQMBAProductsMBAImages(asin=self.get_overview_asin(overview_response_product), url_image_lowq=url_image_lowq,
                                                                     url_image_q2=url_image_q2, url_image_q3=url_image_q3, url_image_q4=url_image_q4, url_image_hq=url_image_hq
                                                                     ))
        return bq_mba_products_mba_images_list

    def get_BQMBAProductsMBARelevance_list(self, overview_response, page, products_per_page=48) -> List[BQMBAProductsMBARelevance]:
        bq_mba_products_mba_relevance_list = []
        for shirt_number_page, overview_response_product in enumerate(self.get_overview_products_response_list(overview_response)):
            number = int(shirt_number_page + ((int(page) - 1) * products_per_page))
            bq_mba_products_mba_relevance_list.append(BQMBAProductsMBARelevance(asin=self.get_overview_asin(overview_response_product), sort=self.sort,
                                                                                number=number))
        return bq_mba_products_mba_relevance_list
