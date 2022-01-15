from typing import List

from mwfunctions.crawler.mw_scrapy.base_classes.spider_base import MBASpider
from mwfunctions.crawler.mw_scrapy.scrapy_selectors import overview as overview_selector
from mwfunctions.pydantic import BQMBAOverviewProduct, BQMBAProductsMbaImages, BQMBAProductsMbaRelevance
from mwfunctions.time import get_berlin_timestamp


class MBAOverviewSpider(MBASpider):

    def __init__(self, sort, pages=0, start_page=1, keyword="", *args, **kwargs):
        # super(MBAOverviewSpider, self).__init__(*args, **kwargs)
        MBASpider.__init__(self, *args, **kwargs)

        self.sort = sort # e.g. newest
        self.keyword = keyword
        self.pages = int(pages)
        self.start_page = int(start_page)

    '''
    ### Parent functions
    '''
    @staticmethod
    def get_zip_code_location(response):
        try:
            return response.css('span#glow-ingress-line2::text').get().strip()
        except:
            return "unkown"

    @staticmethod
    def get_count_results(response):
        try:
            count_results_bar_text = response.css('span.celwidget div.a-section span::text')[0].get()
            return int(count_results_bar_text.split(" results")[0].split(" ")[-1].replace(',',''))
        except:
            return "unkown"

    def should_zip_code_be_changed(self, response):
        if self.marketplace == "com":
            #zip_code_location = self.get_zip_code_location(response)
            zip_code_location = "unkown"
            if zip_code_location == "unkown":
                count_results = self.get_count_results(response)
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