import scrapy
import json
import datetime
from pathlib import Path
from proxy import proxy_handler
import pandas as pd
from re import findall
from bs4 import BeautifulSoup
from google.cloud import bigquery
import sys
sys.path.append("...")
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\flori\\Dropbox\\Apps\\MBA Pipeline\\merchwatch.de\\privacy files\\mba-pipeline-4de1c9bf6974.json"
from proxy.utils import get_random_headers, send_msg
from urllib.parse import urlparse
import dateparser
from scrapy.exceptions import CloseSpider

import time
import threading

# from scrapy.contrib.spidermiddleware.httperror import HttpError
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, ConnectionLost
from twisted.web._newclient import ResponseNeverReceived
from scrapy.core.downloader.handlers.http11 import TunnelError

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
    name = "mba_images"
    Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
    df_mba_images = pd.DataFrame(data={"asin":[],"url_image_lowq":[],"url_image_q2":[], "url_image_q3":[], "url_image_q4":[],"url_image_hq":[], "timestamp":[]})
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []
    captcha_count = 0
    was_banned = {}

    # HINT: should be calles ba settings, since settings will be changed with file string replace
    # custom_settings = {
    #     "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=True),
    # }

    def __init__(self, marketplace, *args, **kwargs):
        self.marketplace = marketplace
        self.allowed_domains = ['amazon.' + marketplace]
        self.url_data_path = f"mba_crawler/url_data/urls_mba_images_{self.marketplace}.csv"
        self.products_mba_image_references_already_crawled = self.get_asin_crawled("mba_%s.products_mba_images" % marketplace)

        # does not work currently
        # if self.marketplace == "com":
        #     self.custom_settings.update({
        #         "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=True),
        #     })
        # self.settings.attributes["ROTATING_PROXY_LIST"] = proxy_handler.get_http_proxy_list(only_usa=True)
        super().__init__(*args, **kwargs)  # python3
        
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

    def start_requests(self):
        self.reset_was_banned_every_hour()
        urls = pd.read_csv(self.url_data_path)["url"].tolist()
        asins = pd.read_csv(self.url_data_path)["asin"].tolist()
        for i, url in enumerate(urls):
            #proxies = proxy_handler.get_random_proxy_url_dict()
            headers = get_random_headers(self.marketplace)
            asin = asins[i]
            yield scrapy.Request(url=url, callback=self.parse, headers=headers, priority=1,
                                    errback=self.errback_httpbin, meta={"asin": asin, "max_proxies_to_try": 20}) # "proxy": proxies["http"], 

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


    def reset_was_banned_every_hour(self):
        self.reset_ban = threading.Timer(1 * 60 * 60, self.reset_was_banned_every_hour)
        self.reset_ban.start()
        if self.was_banned:
            pass
            #send_msg(self.target, "Reset banned proxies", self.api_key)
        self.was_banned = {}

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
        if "perfect-privacy" in proxy:
            if self.get_ban_timestamp(proxy) != None and ((datetime.datetime.now() - self.get_ban_timestamp(proxy)).total_seconds() < (60*5)):
                was_already_banned = True
        else:
            if self.get_ban_timestamp(proxy) != None and ((datetime.datetime.now() - self.get_ban_timestamp(proxy)).total_seconds() < (60*10)):
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
        elif type(exception) == CloseSpider:
            print("Spider should be closed. Sleep 3 minutes")
            time.sleep(60*3)
            return None
        else:
            return None

    def save_content(self, response, asin):
        filename = "data/" + self.name + "/content/%s.html" % asin
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

    def store_df(self):
        filename = "data/" + self.name + "/mba_images_%s.csv" % datetime.datetime.now().date()
        self.df_mba_images.to_csv(filename, index=False)
        self.log('Saved file %s' % filename)

    def is_captcha_required(self, response):
        captcha = "captcha" in response.body.decode("utf-8").lower()
        content_protection = "benningtonschools" in response.body.decode("utf-8").lower()
        if content_protection:
            print("Found content protection of benningtonschools.org")
        return  content_protection or captcha

    def get_proxy(self, response):
        proxy = ""
        if "proxy" in response.meta:
            proxy = response.meta["proxy"]
        return proxy

    def send_request_again(self, url, asin):
        headers = get_random_headers(self.marketplace)
        # send new request with high priority
        request = scrapy.Request(url=url, callback=self.parse, headers=headers, priority=0, dont_filter=True,
                                errback=self.errback_httpbin, meta={"asin": asin})
        yield request

    def is_mba_shirt(self, response):
        # mba shirts have always fit type (Herren, Damen, Kinder)
        return len(response.css('div#variation_fit_type span')) > 0

    def parse(self, response):
        asin = response.meta["asin"]
        proxy = self.get_proxy(response)

        url = response.url
        #send_msg(self.target, "Response catched: {} with proxy {}".format(url,proxy), self.api_key)
        if self.is_captcha_required(response):
            #self.response_is_ban(request, response, is_ban=True)
            print("Captcha required for proxy: " + proxy)
            self.captcha_count = self.captcha_count + 1
            self.update_ban_count(proxy)
            #send_msg(self.target, "Captcha: " + url, self.api_key)
            
            headers = get_random_headers(self.marketplace)
            # send new request with high priority
            request = scrapy.Request(url=url, callback=self.parse, headers=headers, priority=0, dont_filter=True,
                                    errback=self.errback_httpbin, meta={"asin": asin})
            yield request
            '''
            raise Exception("Captcha required")
            send_msg(self.target, "Captcha required" + " | asin: " + asin, self.api_key)
            self.captcha_count = self.captcha_count + 1
            # add download dely if captcha happens
            self.settings.attributes["DOWNLOAD_DELAY"].value = self.settings.attributes["DOWNLOAD_DELAY"].value + 3
            if self.captcha_count > self.settings.attributes["MAX_CAPTCHA_NUMBER"].value:
                raise CloseSpider(reason='To many catchas received')
            raise Exception("Captcha required")
            '''
        else:
            self.ip_addresses.append(response.ip_address.compressed)
            try:
                # TODO: This scipt was not further developed, since image references are not the same on product page and overview page
                import ast
                image_urls_string = response.css('div#imgTagWrapperId img::attr(data-a-dynamic-image)').get()
                image_urls_dict = ast.literal_eval(image_urls_string)
                url_image_lowq, url_image_q2, url_image_q3, url_image_q4, url_image_hq = self.get_price(response)
            except Exception as e:
                #self.save_content(response, asin)
                #send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                raise e

            crawlingdate = datetime.datetime.now()

            df_mba_images = pd.DataFrame(data={"asin":[asin],"url_image_lowq":[url_image_lowq],"url_image_q2":[url_image_q2], "url_image_q3":[url_image_q3], "url_image_q4":[url_image_q4],"url_image_hq":[url_image_hq], "timestamp":[crawlingdate]})
            self.df_mba_images = self.df_mba_images.append(df_mba_images)


    def drop_asins_already_crawled(self):
        self.df_mba_images = self.df_mba_images[~self.df_mba_images['asin'].isin(self.products_mba_image_references_already_crawled)]

    def closed(self, reason):
        try:
            self.reset_ban.cancel()
        except Exception as e:
            send_msg(self.target, "Could not cancel ban reset function", self.api_key)
            print("Could not cancel ban reset function", str(e))
        try:
            ip_dict = {i:self.ip_addresses.count(i) for i in self.ip_addresses}
            ip_addr_str=""
            for ip, count in ip_dict.items():
                ip_addr_str = "{}{}: {}\n".format(ip_addr_str, ip, count)
            proxy_str=""
            for proxy, data in self.was_banned.items():
                proxy_str = "{}{}: {}\n".format(proxy_str, proxy, data[0])
            print(ip_addr_str)
            print(proxy_str)
            ip_addresses_str = "\n".join(list(set(self.ip_addresses)))
            #send_msg(self.target, "Used ip addresses: \n{}".format(ip_addr_str), self.api_key)
            #send_msg(self.target, "Ban count proxies: \n{}".format(proxy_str), self.api_key)
            print(proxy_str)
            print("Used ip addresses: \n{}".format(ip_addr_str))
            print("Ban count proxies: \n{}".format(proxy_str))
            #send_msg(self.target, "Captcha response count: {}".format(self.captcha_count), self.api_key)
        except:
            pass

        self.drop_asins_already_crawled()

        self.df_mba_images['timestamp'] = self.df_mba_images['timestamp'].astype('datetime64[ns]')
        self.df_mba_images = self.df_mba_images.drop_duplicates(["asin"])


        try:
            self.df_mba_images.to_gbq("mba_" + self.marketplace + ".products_mba_images",project_id="mba-pipeline", if_exists="append")
        except:
            time.sleep(10)
            try:
                self.df_mba_images.to_gbq("mba_" + self.marketplace + ".products_mba_images",project_id="mba-pipeline", if_exists="append")
            except:
                self.store_df()
