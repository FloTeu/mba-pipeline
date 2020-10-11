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
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\flori\\Dropbox\\Apps\\MBA Pipeline\\merchwatch.de\\privacy files\\mba-pipeline-4de1c9bf6974.json"
from proxy.utils import get_random_headers, send_msg
from urllib.parse import urlparse
import dateparser

import time

# from scrapy.contrib.spidermiddleware.httperror import HttpError
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError


class AmazonSpider(scrapy.Spider):
    name = "mba_general_de"
    marketplace = "de"
    Path("data/" + name + "/content").mkdir(parents=True, exist_ok=True)
    df_products_details = pd.DataFrame(data={"asin":[],"title":[],"brand":[],"url_brand":[],"price":[], "fit_types": [], "color_names": [],"color_count":[],"product_features": [],"description":[],"weight": [],"upload_date_str": [],"upload_date": [],"customer_review_score": [],"customer_review_count": [],"mba_bsr_str": [],"mba_bsr": [],"mba_bsr_categorie": [],"timestamp": []})
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []

    def start_requests(self):
        urls = pd.read_csv("mba_crawler/urls_small.csv")["url"].tolist()
        asins = pd.read_csv("mba_crawler/urls_small.csv")["asin"].tolist()
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

    def parse(self, response):
        self.ip_addresses.append(response.ip_address.compressed)
        asin = response.meta["asin"]
        try:
            price_str, price = self.get_price(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie = self.get_bsr(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            customer_review_score_mean, customer_review_score, customer_review_count = self.get_customer_review(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            title = self.get_title(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            brand, url_brand = self.get_brand_infos(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            fit_types = self.get_fit_types(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            array_color_names, color_count = self.get_color_infos(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        try:
            array_product_feature = self.get_product_features(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
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
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
        try:
            upload_date_str, upload_date = self.get_upload_date(response)
        except Exception as e:
            self.save_content(response, asin)
            send_msg(self.target, str(e) + "| asin: " + asin, self.api_key)
            raise e
        
        crawlingdate = datetime.datetime.now()

        df = pd.DataFrame(data={"asin": [asin], "title": [title], "brand": [brand], "url_brand": [url_brand], "price": [price_str], "fit_types": [fit_types],
                  "color_names": [array_color_names], "color_count": [color_count], "product_features": [array_product_feature], "description": [description], "weight": [weight],
                  "upload_date_str": [upload_date_str], "upload_date": [upload_date], "customer_review_score": [customer_review_score], "customer_review_count": [customer_review_count],
                  "mba_bsr_str": [mba_bsr_str], "mba_bsr": [mba_bsr], "mba_bsr_categorie": [array_mba_bsr_categorie], "timestamp": [crawlingdate]})
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
        self.df_products_details['upload_date'] = self.df_products_details['upload_date'].astype('datetime64')
        self.df_products_details['mba_bsr'] = self.df_products_details['mba_bsr'].astype('int')
        self.df_products_details['customer_review_count'] = self.df_products_details['customer_review_count'].astype('int')
        # update data in bigquery if batch is finished
        #'''
        try:
            self.df_products_details.to_gbq("mba_" + self.marketplace + ".products_details_general",project_id="mba-pipeline", if_exists="append")
        except:
            time.sleep(10)
            try:
                self.df_products_details.to_gbq("mba_" + self.marketplace + ".products_details_generaö",project_id="mba-pipeline", if_exists="append")
            except:
                self.store_df()
        #'''