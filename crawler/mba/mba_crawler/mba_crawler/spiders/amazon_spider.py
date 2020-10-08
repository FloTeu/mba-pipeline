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
from proxy.utils import get_random_headers, send_msg
import time

# from scrapy.contrib.spidermiddleware.httperror import HttpError
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError

class AmazonSpider(scrapy.Spider):
    name = "amazon_daily_de"
    marketplace = "de"
    Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
    df_products_details = pd.DataFrame(data={"asin":[],"price":[],"price_str":[],"bsr":[],"bsr_str":[], "array_bsr": [], "array_bsr_categorie": [],"customer_review_score_mean":[],"customer_review_score": [],"customer_review_count": [], "timestamp":[]})
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []

    def start_requests(self):
        urls = pd.read_csv("mba_crawler/urls.csv")["url"].tolist()
        asins = pd.read_csv("mba_crawler/urls.csv")["asin"].tolist()
        send_msg(self.target, "Start scraper {} with {} products".format(self.name, len(urls)), self.api_key)
        for i, url in enumerate(urls):
            #proxies = proxy_handler.get_random_proxy_url_dict()
            headers = get_random_headers(self.marketplace)
            asin = asins[i]
            yield scrapy.Request(url=url, callback=self.parse, headers=headers,
                                    errback=self.errback_httpbin, meta={"asin": asin}) # "proxy": proxies["http"], 

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
                    df = pd.DataFrame(data={"asin":[response.meta["asin"]],"price":[404.0],"price_str":["404"],"bsr":[404],"bsr_str":["404"], "array_bsr": [["404"]], "array_bsr_categorie": [["404"]],"customer_review_score_mean":[404.0],"customer_review_score": ["404"],"customer_review_count": [404], "timestamp":crawlingdate})
                    self.df_products_details = self.df_products_details.append(df)
                else:
                    send_msg(self.target, "HttpError on asin: {} | status_code: {} | ip address: {}".format(response.meta["asin"], response.status, response.ip_address.compressed), self.api_key)
            except:
                pass
            self.logger.error('HttpError on %s', response.url)

        #elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            send_msg(self.target, "DNSLookupError on url: {}".format(request.url), self.api_key)
            self.logger.error('DNSLookupError on %s', request.url)

        #elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError):
            request = failure.request
            send_msg(self.target, "TimeoutError on url: {}".format(request.url), self.api_key)
            self.logger.error('TimeoutError on %s', request.url)

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

    def parse(self, response):
        self.ip_addresses.append(response.ip_address.compressed)
        asin = response.meta["asin"]
        try:
            price_str, price = self.get_price(response)
        except Exception as e:
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie = self.get_bsr(response)
        except Exception as e:
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            customer_review_score_mean, customer_review_score, customer_review_count = self.get_customer_review(response)
        except Exception as e:
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        
        crawlingdate = datetime.datetime.now()
        df = pd.DataFrame(data={"asin":[asin],"price":[price],"price_str":[price_str],"bsr":[mba_bsr],"bsr_str":[mba_bsr_str], "array_bsr": [array_mba_bsr], "array_bsr_categorie": [array_mba_bsr_categorie],"customer_review_score_mean":[customer_review_score_mean],"customer_review_score": [customer_review_score],"customer_review_count": [customer_review_count], "timestamp":[crawlingdate]})
        self.df_products_details = self.df_products_details.append(df)

    def closed(self, reason):
        try:
            ip_dict = {i:self.ip_addresses.count(i) for i in self.ip_addresses}
            ip_addr_str=""
            for ip, count in ip_dict.items():
                ip_addr_str = "{}{}: {}\n".format(ip_addr_str, ip, count)
            #ip_addresses_str = "\n".join(list(set(self.ip_addresses)))
            send_msg(self.target, "Used ip addresses: \n{}".format(ip_addr_str), self.api_key)
        except:
            pass
        send_msg(self.target, "Finished scraper {} with {} products".format(self.name, len(self.df_products_details)), self.api_key)
        self.df_products_details['timestamp'] = self.df_products_details['timestamp'].astype('datetime64')
        self.df_products_details['bsr'] = self.df_products_details['bsr'].astype('int')
        self.df_products_details['customer_review_count'] = self.df_products_details['customer_review_count'].astype('int')
        # update data in bigquery if batch is finished
        try:
            self.df_products_details.to_gbq("mba_" + self.marketplace + ".products_details_daily",project_id="mba-pipeline", if_exists="append")
        except:
            time.sleep(10)
            try:
                self.df_products_details.to_gbq("mba_" + self.marketplace + ".products_details_daily",project_id="mba-pipeline", if_exists="append")
            except:
                pass
