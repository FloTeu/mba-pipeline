import scrapy
import json
import datetime
from pathlib import Path
from proxy import proxy_handler
import pandas as pd
from re import findall
from bs4 import BeautifulSoup
import sys
sys.path.append("...")
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\flori\\Dropbox\\Apps\\MBA Pipeline\\merchwatch.de\\privacy files\\mba-pipeline-4de1c9bf6974.json"
from proxy.utils import get_random_headers, send_msg
from urllib.parse import urlparse
import dateparser
from scrapy.exceptions import CloseSpider

import time

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
    name = "mba_general_de"
    marketplace = "de"
    allowed_domains = ['amazon.' + marketplace]
    Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
    df_products_details = pd.DataFrame(data={"asin":[],"title":[],"brand":[],"url_brand":[],"price":[], "fit_types": [], "color_names": [],"color_count":[],"product_features": [],"description":[],"weight": [],"upload_date_str": [],"upload_date": [],"customer_review_score": [],"customer_review_count": [],"mba_bsr_str": [],"mba_bsr": [],"mba_bsr_categorie": [],"timestamp": []})
    df_products_details_daily = pd.DataFrame(data={"asin":[],"price":[],"price_str":[],"bsr":[],"bsr_str":[], "array_bsr": [], "array_bsr_categorie": [],"customer_review_score_mean":[],"customer_review_score": [],"customer_review_count": [], "timestamp":[]})
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []
    captcha_count = 0
    was_banned = {}

    def __init__(self, daily=True, **kwargs):
        self.daily = str2bool(daily)
        if self.daily:
            self.url_data_path = "mba_crawler/url_data/urls_mba_daily_de.csv"
        else:
            self.url_data_path = "mba_crawler/url_data/urls_mba_general_de.csv"

        super().__init__(**kwargs)  # python3

    def start_requests(self):
        urls = pd.read_csv(self.url_data_path)["url"].tolist()
        asins = pd.read_csv(self.url_data_path)["asin"].tolist()
        send_msg(self.target, "Start scraper {} daily {} with {} products".format(self.name, self.daily, len(urls)), self.api_key)
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
        if len(self.df_products_details_daily) % 100 == 0:
            send_msg(self.target, "Crawled {} pages".format(len(self.df_products_details_daily)), self.api_key)

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

    def save_content(self, response, asin):
        filename = "data/" + self.name + "/content/%s.html" % asin
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

    def store_df(self):
        filename = "data/" + self.name + "/%s.csv" % datetime.datetime.now().date()
        self.df_products_details.to_csv(filename, index=False)
        self.log('Saved file %s' % filename)


    def get_price(self, response):
        price_div = response.css('div#price')
        price_str = price_div.css('span#priceblock_ourprice::text').get()
        price = 0.0
        if price_str == None:
            raise ValueError("Could not get price information for crawler " + self.name)
        else:
            try:
                price = float(price_str.split("\xa0")[0].replace(",","."))
            except:
                print("Could not get price as float for crawler " + self.name)

        return price_str, price

    def get_bsr(self, response):
        product_information = response.css('div#dpx-detail-bullets_feature_div')
        bsr_li = product_information.css("li#SalesRank")
        mba_bsr_str = ""
        mba_bsr = 0
        array_mba_bsr = []
        array_mba_bsr_categorie = []
        if bsr_li != None and bsr_li != []:
            try:
                mba_bsr_str = "".join(bsr_li.css("::text").getall()).replace("\n", "")
                bsr_iterator = mba_bsr_str.split("Nr. ")
                bsr_iterator = bsr_iterator[1:len(bsr_iterator)]
                for bsr_str in bsr_iterator:
                    bsr = int(bsr_str.split("in")[0].replace(".", ""))
                    array_mba_bsr.append(bsr)
                    bsr_categorie = bsr_str.split("(")[0].split("in")[1].replace("\xa0", "").strip()
                    array_mba_bsr_categorie.append(bsr_categorie)
                mba_bsr = int(bsr_iterator[0].split("in")[0].replace(".", ""))
            except:
                raise ValueError("Could not get bsr information for crawler " + self.name)

        return mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie
        
    def get_customer_review(self, response):
        product_information = response.css('div#dpx-detail-bullets_feature_div')
        customer_review_div = product_information.css("div#detailBullets_averageCustomerReviews")
        customer_review_score_mean = 0.0
        customer_review_score = ""
        customer_review_count = 0
        if customer_review_div != None and customer_review_div != []:
            try:
                try:
                    customer_review_score = customer_review_div.css("span.a-declarative")[0].css("a")[0].css("i")[0].css("span::text").get()
                except:
                    customer_review_score = ""
                try:
                    customer_review_count = int(customer_review_div.css("span#acrCustomerReviewText::text").get().split(" ")[0])
                except:
                    customer_review_count = 0
                try:
                    customer_review_score_mean = float(customer_review_score.split(" von")[0].replace(",","."))
                except:
                    customer_review_score_mean = 0.0
            except:
                pass

        return customer_review_score_mean, customer_review_score, customer_review_count

    def get_title(self, response):
        title = response.css('span#productTitle::text').get()
        if title == None:
            raise ValueError("Could not get title information for crawler " + self.name)
        else:
            return title.strip()

    def get_brand_infos(self, response):
        brand = response.css('a#bylineInfo::text').get()
        url_brand = response.css('a#bylineInfo::attr(href)').get()
        if brand == None:
            raise ValueError("Could not get brand name for crawler " + self.name)
        if url_brand == None:
            raise ValueError("Could not get brand url for crawler " + self.name)

        parsed_uri = urlparse(response.url)
        mba_base_url = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
        url_brand = mba_base_url + url_brand.strip()
        brand = brand.strip()
        return brand, url_brand

    def get_fit_types(self, response):
        array_fit_types = []
        div_fit_types = response.css('div#variation_fit_type span.a-size-base')
        if div_fit_types != None and len(div_fit_types) > 0:
            for fit_type in div_fit_types:
                array_fit_types.append(fit_type.css("::text").get().strip())
            return array_fit_types
        else:
            try:
                fit_type = response.css('div#variation_fit_type span::text').get().strip()
                array_fit_types.append(fit_type)
                return array_fit_types
            except:
                raise ValueError("Could not get fit types for crawler " + self.name)

    def get_color_infos(self, response):
        array_color_names = []
        span_color_names = response.css('div#variation_color_name span.a-declarative')
        if span_color_names != None and len(span_color_names) > 0:
            for color_name in span_color_names:
                array_color_names.append(color_name.css("img::attr(alt)").get())
            return array_color_names, len(array_color_names)
        else:
            try:
                color = response.css('div#variation_color_name span.selection::text').get().strip()
                array_color_names.append(color)
                return array_color_names, len(array_color_names)
            except:
                raise ValueError("Could not get color names for crawler " + self.name)

    def get_product_features(self, response):
        product_feature = response.css('div#feature-bullets')
        if product_feature == None:
            product_feature = response.css("div#dpx-feature-bullets")
        if product_feature != None:
            array_product_features = []
            for feature in product_feature.css("ul li"):
                array_product_features.append(feature.css("::text").get().strip())
            return array_product_features
        else:
            raise ValueError("Could not get product feature for crawler " + self.name)

    def get_description(self, response):
        product_description = response.css('div#productDescription p::text').get()
        if product_description != None:
            return product_description.strip()
        else:
            raise ValueError("Could not get product description for crawler " + self.name)


    def get_weight(self, response):
        weight = "not found"
        product_information = response.css('div#detail-bullets_feature_div li')
        if product_information == None or product_information == []:
            product_information = response.css('div#dpx-detail-bullets_feature_div li')
        if product_information != None:
            for li in product_information:
                try:
                    info_text = li.css("span span::text").getall()[0].lower()
                    if "gewicht" in info_text or "weight" in info_text or "abmessung" in info_text:
                        weight = li.css("span span::text").getall()[1]
                        return weight.strip()
                except:
                    raise ValueError("Could not get weight for crawler " + self.name)
        else:
            raise ValueError("Could not get weight for crawler " + self.name)

    def get_upload_date(self, response):
        product_information = response.css('div#detail-bullets_feature_div li')
        if product_information == None or product_information == []:
            product_information = response.css('div#dpx-detail-bullets_feature_div li')
        if product_information != None:
            for li in product_information:
                try:
                    info_text = li.css("span span::text").getall()[0].lower()
                    if "seit" in info_text or "available" in info_text:
                        upload_date_str = li.css("span span::text").getall()[1]
                        upload_date = dateparser.parse(upload_date_str).strftime('%Y-%m-%d')
                        return upload_date.strip(), upload_date
                except:
                    raise ValueError("Could not get upload date for crawler " + self.name)
        else:
            raise ValueError("Could not get upload date for crawler " + self.name)

    def is_captcha_required(self, response):
        return "captcha" in response.body.decode("utf-8").lower()

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
                price_str, price = self.get_price(response)
            except Exception as e:
                self.save_content(response, asin)
                send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                raise e
            try:
                mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie = self.get_bsr(response)
            except Exception as e:
                self.save_content(response, asin)
                send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                raise e
            try:
                customer_review_score_mean, customer_review_score, customer_review_count = self.get_customer_review(response)
            except Exception as e:
                self.save_content(response, asin)
                send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                raise e
            # if not daily crawler not everything of website need to be crawled
            if not self.daily:
                try:
                    title = self.get_title(response)
                except Exception as e:
                    self.save_content(response, asin)
                    send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                    raise e
                try:
                    brand, url_brand = self.get_brand_infos(response)
                except Exception as e:
                    self.save_content(response, asin)
                    send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                    raise e
                try:
                    fit_types = self.get_fit_types(response)
                except Exception as e:
                    self.save_content(response, asin)
                    send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                    raise e
                try:
                    array_color_names, color_count = self.get_color_infos(response)
                except Exception as e:
                    self.save_content(response, asin)
                    send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                    raise e
                try:
                    array_product_feature = self.get_product_features(response)
                except Exception as e:
                    self.save_content(response, asin)
                    send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                    raise e
                try:
                    description = self.get_description(response)
                except Exception as e:
                    #self.save_content(response, asin)
                    #send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
                    #raise e
                    description = ""
                try:
                    weight = self.get_weight(response)
                except Exception as e:
                    weight = "not found"
                    self.save_content(response, asin)
                    send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                try:
                    upload_date_str, upload_date = self.get_upload_date(response)
                except Exception as e:
                    self.save_content(response, asin)
                    send_msg(self.target, str(e) + " | asin: " + asin, self.api_key)
                    raise e
                
            crawlingdate = datetime.datetime.now()
            if not self.daily:
                # append to general crawler
                df = pd.DataFrame(data={"asin": [asin], "title": [title], "brand": [brand], "url_brand": [url_brand], "price": [price_str], "fit_types": [fit_types],
                        "color_names": [array_color_names], "color_count": [color_count], "product_features": [array_product_feature], "description": [description], "weight": [weight],
                        "upload_date_str": [upload_date_str], "upload_date": [upload_date], "customer_review_score": [customer_review_score], "customer_review_count": [customer_review_count],
                        "mba_bsr_str": [mba_bsr_str], "mba_bsr": [array_mba_bsr], "mba_bsr_categorie": [array_mba_bsr_categorie], "timestamp": [crawlingdate]})
                self.df_products_details = self.df_products_details.append(df)

            # append to daily crawler
            df = pd.DataFrame(data={"asin":[asin],"price":[price],"price_str":[price_str],"bsr":[mba_bsr],"bsr_str":[mba_bsr_str], "array_bsr": [array_mba_bsr], "array_bsr_categorie": [array_mba_bsr_categorie],"customer_review_score_mean":[customer_review_score_mean],"customer_review_score": [customer_review_score],"customer_review_count": [customer_review_count], "timestamp":[crawlingdate]})
            self.df_products_details_daily = self.df_products_details_daily.append(df)

            self.status_update()

            #if self.captcha_count > self.settings.attributes["MAX_CAPTCHA_NUMBER"].value:
            #    raise CloseSpider(reason='To many captchas received')

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
            send_msg(self.target, "Used ip addresses: \n{}".format(ip_addr_str), self.api_key)
            send_msg(self.target, "Ban count proxies: \n{}".format(proxy_str), self.api_key)
            send_msg(self.target, "Captcha response count: {}".format(self.captcha_count), self.api_key)
        except:
            pass
        send_msg(self.target, "Finished scraper {} daily {} with {} products and reason: {}".format(self.name, self.daily, len(self.df_products_details_daily), reason), self.api_key)
        
        if not self.daily:
            # change types to fit with big query datatypes
            self.df_products_details['color_count'] = self.df_products_details['color_count'].astype('int')
            self.df_products_details['timestamp'] = self.df_products_details['timestamp'].astype('datetime64[ns]')
            self.df_products_details['upload_date'] = self.df_products_details['upload_date'].astype('datetime64[ns]')
            self.df_products_details['customer_review_count'] = self.df_products_details['customer_review_count'].astype('int')


        # change types of daily dataframe
        self.df_products_details_daily['timestamp'] = self.df_products_details_daily['timestamp'].astype('datetime64[ns]')
        self.df_products_details_daily['bsr'] = self.df_products_details_daily['bsr'].astype('int')
        self.df_products_details_daily['customer_review_count'] = self.df_products_details_daily['customer_review_count'].astype('int')

        # update data in bigquery if batch is finished
        if not self.daily:
            try:
                self.df_products_details.to_gbq("mba_" + self.marketplace + ".products_details",project_id="mba-pipeline", if_exists="append")
            except:
                time.sleep(10)
                try:
                    self.df_products_details.to_gbq("mba_" + self.marketplace + ".products_details",project_id="mba-pipeline", if_exists="append")
                except:
                    self.store_df()

        try:
            self.df_products_details_daily.to_gbq("mba_" + self.marketplace + ".products_details_daily",project_id="mba-pipeline", if_exists="append")
        except:
            time.sleep(10)
            try:
                self.df_products_details_daily.to_gbq("mba_" + self.marketplace + ".products_details_daily",project_id="mba-pipeline", if_exists="append")
            except:
                self.store_df()
