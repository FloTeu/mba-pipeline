import scrapy
import json
import datetime
from pathlib import Path
from proxy import proxy_handler
import pandas as pd
import numpy as np
from google.cloud import bigquery
from re import findall
import re
from bs4 import BeautifulSoup
import sys
sys.path.append("...")
sys.path.append("..")
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\flori\\Dropbox\\Apps\\MBA Pipeline\\merchwatch.de\\privacy files\\mba-pipeline-4de1c9bf6974.json"
from proxy.utils import get_random_headers, send_msg
from proxy import proxy_handler
from ..items import MbaCrawlerItem
from urllib.parse import urlparse
from scrapy.exceptions import CloseSpider
#import mba_url_creator as url_creator
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
import mwfunctions.crawler.mba.url_creator as url_creator

environment.set_cloud_logging()
LOGGER = get_logger(__name__, labels_dict={"topic": "crawling", "target": "overview_page", "type": "scrapy"}, do_cloud_logging=True)

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ValueError("Provided argument is not a bool")

class MBASpider(scrapy.Spider):
    name = "mba_overview"
    Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
    df_products = pd.DataFrame(data={"title":[],"brand":[],"url_product":[],"url_image_lowq":[],"url_image_hq":[],"price":[],"asin":[],"uuid":[], "timestamp":[]})
    df_mba_images = pd.DataFrame(data={"asin":[],"url_image_lowq":[],"url_image_q2":[], "url_image_q3":[], "url_image_q4":[],"url_image_hq":[], "timestamp":[]})
    df_mba_relevance = pd.DataFrame(data={"asin":[],"sort":[],"number":[],"timestamp":[]})
    df_search_terms = pd.DataFrame()
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []
    captcha_count = 0
    was_banned = {}
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
 
    custom_settings = {
        # Set by settings.py
        #"ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=False),

        'ITEM_PIPELINES': {
            'mba_crawler.pipelines.MbaCrawlerImagePipeline': 200
        },

        'IMAGES_STORE': 'gs://5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/',
        'GCS_PROJECT_ID': 'mba-pipeline'
    }

    def __init__(self, marketplace, pod_product, sort, keyword="", pages=0, start_page=1, csv_path="", **kwargs):
        self.marketplace = marketplace
        self.pod_product = pod_product
        self.sort = sort
        self.keyword = keyword
        self.pages = int(pages)
        self.start_page = int(start_page)
        self.allowed_domains = ['amazon.' + marketplace]
        self.products_already_crawled = self.get_asin_crawled("mba_%s.products" % marketplace)
        # all image quality url crawled
        self.products_mba_image_references_already_crawled = self.get_asin_crawled("mba_%s.products_mba_images" % marketplace)
        # all images which are already downloaded to storage
        self.products_images_already_downloaded = self.get_asin_crawled("mba_%s.products_images" % marketplace)

        if csv_path != "":
            self.df_search_terms = pd.read_csv(csv_path)

        # does not work currently
        # if self.marketplace == "com":
        #     self.custom_settings.update({
        #         "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=True),
        #     })
        
        super().__init__(**kwargs)  # python3

    def start_requests(self):
        urls_mba = []
        headers = get_random_headers(self.marketplace)
        # case use a csv with search terms
        if not self.df_search_terms.empty:
            for i, df_row in self.df_search_terms.iterrows():
                search_term = df_row["search_term"]
                url_mba = url_creator.main([search_term, self.marketplace, self.pod_product, self.sort])
                url_mba_page = url_mba + "&page=1"#+"&ref=sr_pg_"+str(page_number)
                urls_mba.append(url_mba_page)
        else:
            url_mba = url_creator.main([self.keyword, self.marketplace, self.pod_product, self.sort])
            # send_msg(self.target, "Start scraper {} marketplace {} with {} pages and start page {} and sort {}".format(self.name, self.marketplace, self.pages, self.start_page, self.sort), self.api_key)
            LOGGER.info("Start scraper {} marketplace {} with {} pages and start page {} and sort {}".format(self.name, self.marketplace, self.pages, self.start_page, self.sort))

            # if start_page is other than one, crawler should start from differnt page
            until_page = 401

            if self.pages != 0:
                until_page = self.start_page + self.pages
            for page_number in np.arange(self.start_page, until_page, 1):
                if page_number <= 400:
                    url_mba_page = url_mba + "&page="+str(page_number)#+"&ref=sr_pg_"+str(page_number)
                    urls_mba.append(url_mba_page)
        for i, url_mba in enumerate(urls_mba):
            page = i + self.start_page
            # if self.marketplace == "com": 
            #     url_change_zip_code = "https://www.amazon.com/gp/delivery/ajax/address-change.html"
            #     yield scrapy.http.JsonRequest(url=url_change_zip_code, callback=self.change_zip_code, headers=headers, priority=i, data=self.change_zip_code_post_data,
            #                         errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, 'page': page, "url": url_mba, "headers": headers})
            # else:
            yield scrapy.Request(url=url_mba, callback=self.parse, headers=headers, priority=i,
                                    errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, 'page': page, "url": url_mba, "headers": headers})

    def errback_httpbin(self, failure):
        # log all errback failures,
        # in case you want to do something special for some errors,
        # you may need the failure's type
        self.logger.error(repr(failure))

        #if isinstance(failure.value, HttpError):
        if failure.check(HttpError):
            # you can get the response
            response = failure.value.response
            try:
                # if 404 update big query
                if response.status == 404:
                    crawlingdate = datetime.datetime.now()
                    df = pd.DataFrame(data={"asin":[response.meta["asin"]],"title":["404"],"brand":["404"],"url_brand":["404"],"price":["404"],"fit_types":[["404"]],"color_names":[["404"]],"color_count":[404],"product_features":[["404"]],"description":["404"],"weight": ["404"],"upload_date_str":["1995-01-01"],"upload_date": ["1995-01-01"],"customer_review_score": ["404"],"customer_review_count": [404],"mba_bsr_str": ["404"], "mba_bsr": [["404"]], "mba_bsr_categorie": [["404"]], "timestamp":[crawlingdate]})
                    self.df_products_details = self.df_products_details.append(df)
                    print("HttpError on asin: {} | status_code: {} | ip address: {}".format(response.meta["asin"], response.status, response.ip_address.compressed))
                else:
                    #send_msg(self.target, "HttpError on asin: {} | status_code: {} | ip address: {}".format(response.meta["asin"], response.status, response.ip_address.compressed), self.api_key)
                    print( "HttpError on asin: {} | status_code: {} | ip address: {}".format(response.meta["asin"], response.status, response.ip_address.compressed))
                    proxy = self.get_proxy(response)
                    self.update_ban_count(proxy)
                    #self.send_request_again(response.url, response.meta["asin"])
            except:
                pass
            self.logger.error('HttpError on %s', response.url)

        #elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            proxy = self.get_proxy(request)
            #send_msg(self.target, "DNSLookupError on url: {} proxy: {}".format(request.url, proxy), self.api_key)
            #self.update_ban_count(proxy)
            self.logger.error('DNSLookupError on %s', request.url)
            #self.send_request_again(request.url, request.meta["asin"])

        #elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError):
            request = failure.request
            proxy = self.get_proxy(request)
            #send_msg(self.target, "TimeoutError on url: {} proxy: {}".format(request.url, proxy), self.api_key)
            #self.update_ban_count(proxy)
            self.logger.error('TimeoutError on %s', request.url)
            #self.send_request_again(request.url, request.meta["asin"])
    
        #elif isinstance(failure.value, TimeoutError):
        elif failure.check(TCPTimedOutError):
            request = failure.request
            proxy = self.get_proxy(request)
            #send_msg(self.target, "TimeoutError on url: {} proxy: {}".format(request.url, proxy), self.api_key)
            #self.update_ban_count(proxy)
            self.logger.error('TCPTimeoutError on %s', request.url)
            #self.send_request_again(request.url, request.meta["asin"])
    
        #elif isinstance(failure.value, TimeoutError):
        elif failure.check(TunnelError):
            request = failure.request
            proxy = self.get_proxy(request)
            #send_msg(self.target, "TimeoutError on url: {} proxy: {}".format(request.url, proxy), self.api_key)
            #self.update_ban_count(proxy)
            self.logger.error('TunnelError on %s', request.url)
            #self.send_request_again(request.url, request.meta["asin"])


    def status_update(self):
        if self.page_count % 10 == 0:
            #send_msg(self.target, "Crawled {} pages".format(int(self.page_count)), self.api_key)
            pass

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
            self.was_banned[proxy] = [self.get_ban_count(proxy) + 1, datetime.datetime.now()]
        else:
            self.was_banned.update({proxy: [1, datetime.datetime.now()] })

    def was_already_banned(self, proxy):
        was_already_banned = False
        # should be banned if captcha was found and it was found in the last 30 minutes
        if self.get_ban_timestamp(proxy) != None and ((datetime.datetime.now() - self.get_ban_timestamp(proxy)).total_seconds() < (60*30)):
            was_already_banned = True
        return was_already_banned

        #if self.get_ban_timestamp(proxy) == None:
        #    yield request
        # last ban need to be longer away than one minute to prevent request loops
        #elif (datetime.datetime.now() - self.get_ban_timestamp(proxy)).total_seconds() > 60:
        #    yield request

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
        else:
            return None

    def get_asin_crawled(self, table_id):
        '''
            Returns a unique list of asins that are already crawled
        '''
        bq_client = bigquery.Client(project='mba-pipeline')
        # todo: change table name in future | 
        try:
            list_asin = bq_client.query("SELECT asin FROM " + table_id + " group by asin").to_dataframe().drop_duplicates(["asin"])["asin"].tolist()
        except Exception as e:
            print(str(e))
            list_asin = []
        return list_asin

    def save_content(self, response, url):
        filename = "data/" + self.name + "/content/%s.html" % url.replace("/","_")
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

    def store_df(self):
        filename = "data/" + self.name + "/products_%s.csv" % datetime.datetime.now().date()
        self.df_products.to_csv(filename, index=False)
        filename = "data/" + self.name + "/mba_images_%s.csv" % datetime.datetime.now().date()
        self.df_mba_images.to_csv(filename, index=False)
        filename = "data/" + self.name + "/mba_relevance%s.csv" % datetime.datetime.now().date()
        self.df_mba_relevance.to_csv(filename, index=False)
        self.log('Saved file %s' % filename)

    def log_error(self, e, custom_msg):
        LOGGER.error(f"{custom_msg}. \nError message: {e}. \nTraceback {traceback.format_exc()}")

    def log_warning(self, e, custom_msg):
        LOGGER.warning(f"{custom_msg}. \nError message: {e}. \nTraceback {traceback.format_exc()}")

    def is_captcha_required(self, response):
        return "captcha" in response.body.decode("utf-8").lower()

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

    def get_title(self, response_shirt):
        title = response_shirt.css("a.a-link-normal.a-text-normal")[0].css("span::text").get()
        if title == None:
            raise ValueError("Could not get title information for crawler " + self.name)
        else:
            return title.strip()

    def get_brand(self, response_shirt):
        brand = response_shirt.css("h5.s-line-clamp-1 span::text")[0].get()
        if brand == None:
            raise ValueError("Could not get brand information for crawler " + self.name)
        else:
            return brand.strip()

    def get_url_product(self, response_shirt, url_mba):
        url_product = response_shirt.css("div.a-section.a-spacing-none a::attr(href)").get()
        if url_product == None:
            raise ValueError("Could not get url_product information for crawler " + self.name)
        else:
            return "/".join(url_mba.split("/")[0:3]) + url_product

    def get_img_urls(self, response_shirt):
        is_url = re.compile(
                r'^(?:http|ftp)s?://' # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                r'localhost|' #localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                r'(?::\d+)?' # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        urls = response_shirt.css("div.a-section a img::attr(srcset)").get()
        if urls == None:
            raise ValueError("Could not get img_urls information for crawler " + self.name)
        else:
            img_url = []
            for url in urls.split(" "):
                if re.match(is_url, url) is not None:
                    img_url.append(url)
            if len(img_url) == 5:
                return img_url[0],img_url[1],img_url[2],img_url[3],img_url[4]
            else:
                raise ValueError("Could not get all 5 img_urls information for crawler " + self.name)

    def get_price(self, response_shirt):
        if self.marketplace == "com":
            price = response_shirt.css("span.a-price-whole::text")[0].get() + response_shirt.css("span.a-price-decimal::text")[0].get() + response_shirt.css("span.a-price-fraction::text")[0].get()
        else:
            price = response_shirt.css("span.a-price-whole::text")[0].get()
        if price == None:
            raise ValueError("Could not get price information for crawler " + self.name)
        else:
            return price.strip()
            
    def get_asin(self, response_shirt):
        asin = response_shirt.xpath("..").attrib["data-asin"]
        if asin == None:
            raise ValueError("Could not get asin for crawler " + self.name)
        else:
            return asin.strip()
            
    def get_uuid(self, response_shirt):
        uuid = response_shirt.xpath("..").attrib["data-uuid"]
        if uuid == None:
            raise ValueError("Could not get uuid for crawler " + self.name)
        else:
            return uuid.strip()

    def is_shirt(self, response_shirt):
        try:
            asin = self.get_asin(response_shirt)
            return True
        except:
            return False

    def get_zip_code_location(self, response):
        try:
            return response.css('span#glow-ingress-line2::text').get().strip()
        except:
            return "unkown"

    def get_count_results(self, response):
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

    def change_zip_code(self, response):
        proxy = self.get_proxy(response)
        if self.is_perfect_privacy_proxy(response):
            proxy = response.meta["proxy"]
        print(proxy)
        meta_dict = response.meta
        meta_dict.update({"proxy": proxy, "_rotating_proxy": False})
        yield response.follow(url=response.meta["url"], callback=self.parse, headers=response.meta["headers"], priority=0,
                                    errback=self.errback_httpbin, meta=meta_dict, dont_filter=True)
        test = 0
        # proxies = {
        #     "http":"http://nwtrs2017:hb7043GesRoP@" + response.meta["download_slot"] + ":3128",
        #     "https":"http://nwtrs2017:hb7043GesRoP@" + response.meta["download_slot"] + ":3128"
        # }
        # r = requests.get(response.meta["url"], headers=response.meta["headers"], proxies=proxies)
        # return scrapy.FormRequest.from_response(
        #     response,
        #     formdata=form_dict,
        #     callback=self.parse,
        #     #meta=response.meta
        # )

    def parse(self, response):
        proxy = self.get_proxy(response)
        url = response.url
        page = response.meta["page"]
        image_urls = []
        asins = []
        url_mba_lowqs = []

        #self.get_zip_code_location(response)
        #self.get_count_results(response)

        if self.is_captcha_required(response):
            #self.response_is_ban(request, response, is_ban=True)
            print("Captcha required for proxy: " + proxy)
            self.captcha_count = self.captcha_count + 1
            self.update_ban_count(proxy)            
            headers = get_random_headers(self.marketplace)
            # send new request with high priority
            request = scrapy.Request(url=response.meta["url"], callback=self.parse, headers=headers, priority=0, dont_filter=True,
                                    errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, "page": page, "url": response.meta["url"]})
            yield request
        else:
            
            if self.should_zip_code_be_changed(response):
                print("Proxy does not get all .com results: " + proxy)
                self.update_ban_count(proxy)   
                headers = get_random_headers(self.marketplace)
                # send new request with high priority
                request = scrapy.Request(url=url, callback=self.parse, headers=headers, priority=0, dont_filter=True,
                                        errback=self.errback_httpbin, meta={"max_proxies_to_try": 30, "page": page})
                yield request
                # change zip code
                # meta_dict = {"max_proxies_to_try": 30, 'page': page, "url": url, "headers": response.meta["headers"]}
                # url_change_zip_code = "https://www.amazon.com/gp/delivery/ajax/address-change.html"
                # if self.is_perfect_privacy_proxy(response):
                #     proxy = "http://nwtrs2017:hb7043GesRoP@" + response.meta["download_slot"] + ":3128"
                # meta_dict.update({"proxy": proxy, "_rotating_proxy": False})
                # yield scrapy.http.JsonRequest(url=url_change_zip_code, callback=self.change_zip_code, headers=response.meta["headers"], priority=0, data=self.change_zip_code_post_data,
                #                     errback=self.errback_httpbin, meta=meta_dict, dont_filter=True)
            else:
                self.ip_addresses.append(response.ip_address.compressed)
                shirts = response.css('div.sg-col-inner')
                shirt_number_page = 0
                for i, shirt in enumerate(shirts):
                    if not self.is_shirt(shirt):
                        continue
                    shirt_number_page = shirt_number_page + 1
                    try:
                        price = self.get_price(shirt)
                    except Exception as e:
                        self.log_error(e, "Could not get price data")
                        #self.save_content(response, url)
                        #send_msg(self.target, str(e) + " | url: " + url, self.api_key)
                        raise e
                    try:
                        title = self.get_title(shirt)
                    except Exception as e:
                        self.log_error(e, "Could not get title")
                        #self.save_content(response, url)
                        #send_msg(self.target, str(e) + " | url: " + url, self.api_key)
                        raise e
                    try:
                        brand = self.get_brand(shirt)
                    except Exception as e:
                        self.log_warning(e, "Could not get brand")
                        #print("Could not get brand of shirt: ",title)
                        brand = None
                        # its possible that amazon does not show brand on overview page. Therefore raise is not neccessary.
                        #self.save_content(response, url)
                        #send_msg(self.target, str(e) + " | url: " + url, self.api_key)
                        #raise e
                    try:
                        url_product = self.get_url_product(shirt, url)
                    except Exception as e:
                        self.log_error(e, "Could not get url of product")
                        #self.save_content(response, url)
                        #send_msg(self.target, str(e) + " | url: " + url, self.api_key)
                        raise e
                    try:
                        url_image_lowq,url_image_q2,url_image_q3,url_image_q4,url_image_hq = self.get_img_urls(shirt)
                    except Exception as e:
                        self.log_error(e, "Could not get image urls")
                        #self.save_content(response, url)
                        #send_msg(self.target, str(e) + " | url: " + url, self.api_key)
                        raise e
                    try:
                        asin = self.get_asin(shirt)
                    except Exception as e:
                        self.log_error(e, "Could not get asin of product")
                        #self.save_content(response, url)
                        #send_msg(self.target, str(e) + " | url: " + url, self.api_key)
                        raise e
                    try:
                        uuid = self.get_uuid(shirt)
                    except Exception as e:
                        self.log_error(e, "Could not get uuid of product")
                        #self.save_content(response, url)
                        #send_msg(self.target, str(e) + " | url: " + url, self.api_key)
                        raise e
                        
                    crawlingdate = datetime.datetime.now()
                    # append to general crawler
                    df_products = pd.DataFrame(data={"title":[title],"brand":[brand],"url_product":[url_product],"url_image_lowq":[url_image_lowq],"url_image_hq":[url_image_hq],"price":[price],"asin":[asin],"uuid":[uuid], "timestamp":[crawlingdate]})
                    df_mba_images = pd.DataFrame(data={"asin":[asin],"url_image_lowq":[url_image_lowq],"url_image_q2":[url_image_q2], "url_image_q3":[url_image_q3], "url_image_q4":[url_image_q4],"url_image_hq":[url_image_hq], "timestamp":[crawlingdate]})
                    shirt_number = int(shirt_number_page + ((int(page)-1)*self.shirts_per_page))
                    df_mba_relevance = pd.DataFrame(data={"asin":[asin],"sort":[self.sort],"number":[shirt_number],"timestamp":[crawlingdate]})

                    self.df_products = self.df_products.append(df_products)
                    self.df_mba_images = self.df_mba_images.append(df_mba_images)
                    self.df_mba_relevance = self.df_mba_relevance.append(df_mba_relevance)

                    # crawl only image if not already crawled
                    if asin not in self.products_images_already_downloaded:
                        image_urls.append(url_image_hq)
                        asins.append(asin)
                        url_mba_lowqs.append(url_image_lowq)

                # crawl images
                image_item = MbaCrawlerItem()
                image_item["image_urls"] = image_urls
                image_item["asins"] = asins
                image_item["url_mba_lowqs"] = url_mba_lowqs
                image_item["marketplace"] = self.marketplace
                if self.marketplace in ["com", "de"]:
                    yield image_item
                
                self.page_count = self.page_count + 1
                self.status_update()


                #url_next = "/".join(url.split("/")[0:3]) + response.css("ul.a-pagination li.a-last a::attr(href)").get()
                
                '''
                if int(self.pages) != 0 and int(self.pages) == self.page_count:
                    raise CloseSpider(reason='Max number of Pages achieved')
                else:
                    self.page_count = self.page_count + 1
                    headers = get_random_headers(self.marketplace)
                    yield scrapy.Request(url=url_next, callback=self.parse, headers=headers, priority=self.page_count,
                                                errback=self.errback_httpbin, meta={"max_proxies_to_try": 30}) 
                '''
                #if self.captcha_count > self.settings.attributes["MAX_CAPTCHA_NUMBER"].value:
                #    raise CloseSpider(reason='To many captchas received')

    def drop_asins_already_crawled(self):
        self.df_products = self.df_products[~self.df_products['asin'].isin(self.products_already_crawled)]
        self.df_mba_images = self.df_mba_images[~self.df_mba_images['asin'].isin(self.products_mba_image_references_already_crawled)]

    def closed(self, reason):
        try:
            ip_dict = {i:self.ip_addresses.count(i) for i in self.ip_addresses}
            ip_addr_str=""
            for ip, count in ip_dict.items():
                ip_addr_str = "{}{}: {}\n".format(ip_addr_str, ip, count)
            proxy_str=""
            for proxy, data in self.was_banned.items():
                proxy_str = "{}{}: {}\n".format(proxy_str, proxy, data[0])
            #ip_addresses_str = "\n".join(list(set(self.ip_addresses)))
            print("Used ip addresses: \n{}".format(ip_addr_str))
            print( "Ban count proxies: \n{}".format(proxy_str))
            print( "Captcha response count: {}".format(self.captcha_count))
            #send_msg(self.target, "Used ip addresses: \n{}".format(ip_addr_str), self.api_key)
            #send_msg(self.target, "Ban count proxies: \n{}".format(proxy_str), self.api_key)
            #send_msg(self.target, "Captcha response count: {}".format(self.captcha_count), self.api_key)
        except:
            pass

        self.drop_asins_already_crawled()

        #send_msg(self.target, "Finished scraper {} with {} new products {} new images {} pages and reason: {}".format(self.name, len(self.df_products), len(self.df_mba_images), self.page_count, reason), self.api_key)
        LOGGER.info("Finished scraper {} with {} new products {} new images {} pages and reason: {}".format(self.name, len(self.df_products), len(self.df_mba_images), self.page_count, reason))

        # change types to fit with big query datatypes
        self.df_products['timestamp'] = self.df_products['timestamp'].astype('datetime64[ns]')
        self.df_mba_images['timestamp'] = self.df_mba_images['timestamp'].astype('datetime64[ns]')
        self.df_mba_relevance['timestamp'] = self.df_mba_relevance['timestamp'].astype('datetime64[ns]')
        self.df_mba_relevance['number'] = self.df_mba_relevance['number'].astype('int')
        
        # drop duplicates by asin
        self.df_products = self.df_products.drop_duplicates(["asin"])
        self.df_mba_images = self.df_mba_images.drop_duplicates(["asin"])

 
        try:
            self.df_products.to_gbq("mba_" + self.marketplace + ".products",project_id="mba-pipeline", if_exists="append")
        except:
            time.sleep(10)
            try:
                self.df_products.to_gbq("mba_" + self.marketplace + ".products",project_id="mba-pipeline", if_exists="append")
            except:
                self.store_df()

        try:
            self.df_mba_images.to_gbq("mba_" + self.marketplace + ".products_mba_images",project_id="mba-pipeline", if_exists="append")
        except:
            time.sleep(10)
            try:
                self.df_mba_images.to_gbq("mba_" + self.marketplace + ".products_mba_images",project_id="mba-pipeline", if_exists="append")
            except:
                self.store_df()

        try:
            self.df_mba_relevance.to_gbq("mba_" + self.marketplace + ".products_mba_relevance",project_id="mba-pipeline", if_exists="append")
        except:
            time.sleep(10)
            try:
                self.df_mba_relevance.to_gbq("mba_" + self.marketplace + ".products_mba_relevance",project_id="mba-pipeline", if_exists="append")
            except:
                self.store_df()
