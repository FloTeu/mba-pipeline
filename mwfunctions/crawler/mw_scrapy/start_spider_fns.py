import os
import sys
import json
import subprocess

from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess, CrawlerRunner, Crawler
from multiprocessing import Process, Queue
from twisted.internet import reactor
from pathlib import Path
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_overview_spider import MBAShirtOverviewSpider as mba_overview_spider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_product_general_spider import MBALocalProductSpider as mba_product_spider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_image_spider import MBAImageSpider as mba_image_spider
from mwfunctions.crawler.mw_scrapy.tests import TestingSpider
from mwfunctions.crawler.mw_scrapy import run_mba_spider
from mwfunctions.pydantic.crawling_classes import CrawlingMBARequest, CrawlingMBAOverviewRequest, CrawlingMBAProductRequest
from mwfunctions.image.conversion import dict2b64_str
from mwfunctions.environment import get_gcp_project
from os import system
from enum import Enum
from pydantic import BaseModel, Field
from typing import Union
import time

#from mwfunctions.crawler.mw_scrapy import run_mba_spider


# class ScrapyMBASpider(Enum):
#     OVERVIEW = mba_overview_spider
#     PRODUCT = mba_product_spider
#     TESTING = TestingSpider
#
#
# class ScrapyMBASpiderItem(BaseModel):
#     spider_module: object = Field(description="Class of spider i.e. imported class")
#     crawling_data_class: Union[CrawlingMBAOverviewRequest, CrawlingMBAProductRequest]
#
#     class Config:
#         arbitrary_types_allowed = True


class ScrapyMBASpider(Enum):
    OVERVIEW = mba_overview_spider
    PRODUCT = mba_product_spider
    IMAGE = mba_image_spider
    TESTING = TestingSpider

#https://stackoverflow.com/questions/4417546/constantly-print-subprocess-output-while-process-is-running
def execute(cmd):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)

# Similar problem (twisted.internet.error.ReactorNotRestartable): https://stackoverflow.com/questions/41495052/scrapy-reactor-not-restartable
class Scraper:
    def __init__(self, spider: ScrapyMBASpider):
        # TODO. change working dir to dir crawler
        # TODO. appand sys path to dir crawler
        # change working directory to spider project root dir
        crawler_dir = "/".join(str(Path(__file__)).split("/")[0:-1])
        os.chdir(crawler_dir)
        sys.path.append(crawler_dir)
        settings_file_path = 'mba_crawler.settings' # The path seen from root, ie. from main.py
        os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
        #self.process = CrawlerProcess(get_project_settings())
        self.crawling_type = spider.name
        self.spider = spider.value # The spider you want to crawl

    def run_spider_handle_twisted_reactor(self, crawling_mba_request: CrawlingMBARequest, url_data_path=None):
        # start multiple process even if twisted reactor is already started (https://stackoverflow.com/questions/41495052/scrapy-reactor-not-restartable)
        def f(q):
            try:
                runner = CrawlerRunner(get_project_settings())
                deferred = runner.crawl(self.spider, crawling_mba_request, url_data_path=url_data_path)
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

    def run_spider(self, crawling_mba_request: CrawlingMBARequest, url_data_path=None, wait_until_finished=True, wait_n_minutes=None):
        # if debug use normal spider call, because run_spider_handle_twisted_reactor does not work correctly for debug mode

        # json_file_path = f'{os.getcwd()}/data/crawling_mba_request.json'
        # with open(json_file_path, 'w') as fp:
        #     json.dump(crawling_mba_request.dict(), fp)

        # crawling_mba_request_str = json.dumps(crawling_mba_request.dict(), indent=2)
        #crawling_mba_request_str = crawling_mba_request.json().replace('"','\"')
        crawling_mba_request_str = json.dumps(json.dumps(crawling_mba_request.dict()))[1:-1]
        #crawling_mba_request_b64_str = base64.urlsafe_b64encode(str().encode(crawling_mba_request.dict())).decode()

        #run_mba_spider.main(json_file_path, self.crawling_type)
        if crawling_mba_request.debug:
            process = CrawlerProcess(get_project_settings())
            process.crawl(self.spider, crawling_mba_request, url_data_path=url_data_path)
            process.start(stop_after_crawl=True)  # the script will block here until the crawling is finished
        else:
            process = subprocess.Popen(f"python3 run_mba_spider.py {self.crawling_type} {dict2b64_str(crawling_mba_request.dict())}".split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT) #, stdout=subprocess.PIPE)
            #process = subprocess.Popen(f"python3 run_mba_spider.py {self.crawling_type} {dict2b64_str(crawling_mba_request.dict())}".split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )

            # for path in execute(f"python3 run_mba_spider.py {self.crawling_type} {dict2b64_str(crawling_mba_request.dict())}".split()):
            #     print(path, end="")
            # TODO find out why process does not finish correctly in local instance on google cloud
            if wait_n_minutes:
                time.sleep(wait_n_minutes * 60)
                process.kill()
                print(f"Waiting time of {wait_n_minutes} min reached")
            elif wait_until_finished:
                process.wait()

                output, errors = process.communicate()
                output_str = output.decode("utf-8")
                if "Error" in output_str or "Exception" in output_str:
                    print("Error found: ", output_str)
        test = 1
        # else:
        #     self.run_spider_handle_twisted_reactor(crawling_mba_request, url_data_path=url_data_path)

    def get_spider(self, crawling_mba_request: CrawlingMBARequest):
        crawler = Crawler(self.spider, settings=get_project_settings())
        return self.spider.from_crawler(crawler, crawling_mba_request)
        #self.spider(crawling_mba_request)

        # runner = CrawlerRunner(get_project_settings())
        # deferred = runner.crawl(self.spider, crawling_mba_request)
        # process = CrawlerProcess(get_project_settings())
        # process.crawl(self.spider, crawling_mba_request)



    def run_spider_old(self, crawling_mba_request: CrawlingMBARequest, url_data_path=None):
        # init of spider
        deferred = self.process.crawl(self.spider, crawling_mba_request, url_data_path=url_data_path)
        spider = next(iter(self.process.crawlers)).spider
        #if spider.debug:
        # start_requests of spider
        # if started twice twisted.internet.error.ReactorNotRestartable happens
        #process_reactor = self.process.start(stop_after_crawl=True)  # the script will block here until the crawling is finished
        # else:
        #     crawling_bash_cmd = f'scrapy crawl {spider.name}'
        #     for spider_param, spider_param_value in crawling_mba_request.dict().items():
        #         crawling_bash_cmd = crawling_bash_cmd + f" -a {spider_param}={spider_param_value}"
        #     system(crawling_bash_cmd)
        #self.process.stop()

        # this works!!
        #process = subprocess.Popen("python3 go_daily_mba_spider.py".split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT) #, stdout=subprocess.PIPE)

        #run_mba_spider.main(self.spider, crawling_mba_request)

        #output, error = process.communicate()
        #system()
        # process = CrawlerProcess(get_project_settings())
        # process.crawl(self.spider, crawling_mba_request)
        # process.start()
        test = 0
        # make sure process is stoped
        #self.process._stop_reactor() #  triggers close() function of spider


if __name__ == "__main__":
    pass


############ deprecated

# process = CrawlerProcess(settings={
#     "FEEDS": {
#         "items.json": {"format": "json"},
#     },
# })
# # Tutorial : https://stackoverflow.com/questions/31662797/getting-scrapy-project-settings-when-script-is-outside-of-root-directory
# def start_overview_spider(crawling_mba_overview, csv_path=""):
#     """
#     pod_product = "shirt"
#     sort = "newest","best_seller"
#     """
#
#     #system(f'scrapy crawl mba_overview -a marketplace={crawling_mba_overview.marketplace} -a pod_product={crawling_mba_overview.pod_product} -a sort={crawling_mba_overview.sort} -a keyword={crawling_mba_overview.keyword} -a pages={crawling_mba_overview.pages} -a start_page={crawling_mba_overview.start_page} -a csv_path={csv_path}')
#     process.crawl(mba_overview_spider, marketplace=crawling_mba_overview.marketplace, pod_product=crawling_mba_overview.pod_product, sort=crawling_mba_overview.sort, keyword=crawling_mba_overview.keyword, pages=crawling_mba_overview.pages, start_page=crawling_mba_overview.start_page, csv_path=csv_path)
#     process.start() # the script will block here until the crawling is finished
#
#
# def start_product_spider(marketplace, daily=True):
#     process.crawl(mba_product_spider, marketplace=marketplace, daily=daily)
#     process.start() # the script will block here until the crawling is finished
#
#     #Scraper(ScrapyMBASpider.OVERVIEW)
#     #start_overview_spider("de", "shirt", "newest", keyword="", pages=1, start_page=1, csv_path="")