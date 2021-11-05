from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_product_general_spider import MBALocalProductSpider as mba_product_spider
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingMBARequest, CrawlingMBAOverviewRequest
import requests


from mwfunctions.crawler.mw_scrapy.spider_base import MBAProductSpider
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingType, CrawlingInputItem
from mwfunctions.crawler.proxy.utils import get_random_headers, send_msg
from mwfunctions.pydantic.bigquery_classes import BQMBAProductsDetails, BQMBAProductsDetailsDaily, BQMBAProductsNoBsr, BQMBAProductsNoMbaShirt
from mwfunctions.io import str2bool
from mwfunctions import environment
from mwfunctions.logger import get_logger

import scrapy
from scrapy.crawler import CrawlerRunner
from multiprocessing import Process, Queue
from twisted.internet import reactor
import os
from pathlib import Path



environment.set_cloud_logging()
LOGGER = get_logger(__name__, labels_dict={"topic": "crawling", "target": "product_page", "type": "scrapy"}, do_cloud_logging=True)

# your spider
class TestingSpider(MBAProductSpider):
    # simple spider can be used to find bugs
    name = "testing"
    #start_urls = ['http://quotes.toscrape.com/tag/humor/']
    website_crawling_target = CrawlingType.PRODUCT.value
    target="869595848"
    api_key="1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0"
    ip_addresses = []
    captcha_count = 0
    page_count = 0
    was_banned = {}

    def __init__(self, mba_product_request: CrawlingMBAProductRequest, url_data_path=None, *args, **kwargs):
        super(TestingSpider, self).__init__(*args, **mba_product_request.dict())
        print(mba_product_request)
        self.daily = str2bool(mba_product_request.daily)
        self.allowed_domains = ['amazon.' + self.marketplace]
        self.mba_product_request = mba_product_request
        self.url_data_path = url_data_path

        self.mba_product_request = mba_product_request

    def start_requests(self):
        self.reset_was_banned_every_hour()
        headers = get_random_headers(self.marketplace)
        #yield scrapy.Request(url='http://quotes.toscrape.com/tag/humor/', callback=self.parse, headers=headers, priority=1,
        #                        errback=self.errback_httpbin, meta={})

        urls = ["https://www.amazon.de/dp/B09K6D2YVZ"]
        asins = ["B09K6D2YVZ"]
        # send_msg(self.target, "Start scraper {} daily {} with {} products".format(self.name, self.daily, len(urls)), self.api_key)
        LOGGER.info("Start scraper {} daily {} with {} products".format(self.name, self.daily, len(urls)))
        print("Start scraper {} daily {} with {} products".format(self.name, self.daily, len(urls)))
        self.crawling_job.number_of_target_pages = len(urls)
        for asin,url in zip(asins, urls):
            yield scrapy.Request(url=url, callback=self.parse_shirt, headers=headers, priority=1,
                             errback=self.errback_httpbin, meta={"asin": asin, "max_proxies_to_try": 20, "url": url})

    def get_proxy(self, response):
        proxy = ""
        if "proxy" in response.meta:
            proxy = response.meta["proxy"]
        return proxy

    def parse(self, response):
        print("PROXY", self.get_proxy(response))
        for quote in response.css('div.quote'):
            print(quote.css('span.text::text').extract_first())

    def status_update(self):
        if self.page_count % 100 == 0:
            print("Crawled {} pages".format(self.page_count))

    def parse_shirt(self, response):
        try:
            asin = response.meta["asin"]
            proxy = self.get_proxy(response)
            print("ASIN", asin, "PROXY", proxy)

            url = response.url
            if self.is_captcha_required(response):
                self.yield_again_if_captcha_required(url, proxy, asin=asin)
            # do not proceed if its not a mba shirt
            elif not self.is_mba_shirt(response):
                self.crawling_job.count_inc("response_successful_count")
                yield BQMBAProductsNoMbaShirt(asin=asin, url=url)
            else:
                self.crawling_job.count_inc("response_successful_count")
                self.ip_addresses.append(response.ip_address.compressed)

                if self.daily:
                    bq_mba_products_details_daily: BQMBAProductsDetailsDaily = self.get_BQMBAProductsDetailsDaily(response, asin)
                    print(bq_mba_products_details_daily)
                    yield bq_mba_products_details_daily
                else:
                    bq_mba_products_details: BQMBAProductsDetails = self.get_BQMBAProductsDetails(response, asin)
                    yield bq_mba_products_details

                self.page_count = self.page_count + 1

                self.status_update()

            # yield no_bsr_products
            while len(self.no_bsr_products) > 0:
                yield self.no_bsr_products.pop(0)

        except Exception as e:
            self.crawling_job.finished_with_error = True
            self.crawling_job.error_msg = str(e)

class Scraper2:
    def __init__(self):
        crawler_dir = "/".join(str(Path(__file__)).split("/")[0:-1])
        os.chdir(crawler_dir)
        sys.path.append(crawler_dir)
        settings_file_path = 'mba_crawler.settings' # The path seen from root, ie. from main.py
        os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
        self.process = CrawlerProcess(get_project_settings())
        self.spider = TestingSpider

    # the wrapper to make it run more times
    def run_spider(self, crawling_mba_request):
        def f(q):
            try:
                runner = CrawlerRunner(get_project_settings())
                deferred = runner.crawl(self.spider, crawling_mba_request)
                deferred.addBoth(lambda _: reactor.stop())
                reactor.run()
                q.put(None)
            except Exception as e:
                q.put(e)

        q = Queue()
        p = Process(target=f, args=(q,))
        p.start()
        result = q.get()
        p.join()

        if result is not None:
            raise result

if __name__ == '__main__':
    import sys
    from api_key import API_KEY

    crawling_mba_overview_request = CrawlingMBAOverviewRequest(marketplace="de", debug=False, sort="newest", pod_product="shirt", pages=4, start_page=44)
    try:
        # r = requests.post(f"https://mw-crawler-api-ruzytvhzvq-ey.a.run.app/start_mba_overview_crawler?wait_until_finished=true&access_token={API_KEY}", crawling_mba_overview_request.json(), timeout=6)
        r = requests.post(f"http://0.0.0.0:8080/start_mba_overview_crawler?wait_until_finished=true&access_token={API_KEY}", crawling_mba_overview_request.json(), timeout=6)
    except:
        pass

    crawling_mba_request = CrawlingMBAProductRequest(marketplace="de", daily=False, number_products=10, top_n=60,
                                                     debug=False, proportions={
            "best_seller": 0.7,
            "lowest_bsr_count": 0.2,
            "random": 0.1
        })
    try:
        #r = requests.post(f"https://mw-crawler-api-ruzytvhzvq-ey.a.run.app/start_mba_product_crawler?wait_until_finished=true&access_token={API_KEY}", crawling_mba_request.json(), timeout=6)
        r = requests.post(f"http://0.0.0.0:8080/start_mba_product_crawler?wait_until_finished=true&access_token={API_KEY}", crawling_mba_request.json(), timeout=6)
    except:
        pass

    test = 1

    # scraper = Scraper(ScrapyMBASpider.PRODUCT)
    # scraper.run_spider(crawling_mba_request)
    # print("SPIDER IS FINISHED")
    # scraper.run_spider(crawling_mba_request)

    # scraper = Scraper2()
    # print('first run:')
    # scraper.run_spider(crawling_mba_request)
    # print('\nsecond run:')
    # scraper.run_spider(crawling_mba_request)

