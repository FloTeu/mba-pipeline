import os
import sys

from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess
from pathlib import Path
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_overview_spider import MBAShirtOverviewSpider as mba_overview_spider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_product_general_spider import MBALocalProductSpider as mba_product_spider
from mwfunctions.pydantic.crawling_classes import CrawlingMBARequest
from os import system
from enum import Enum

class ScrapySpider(Enum):
    OVERVIEW = mba_overview_spider
    PRODUCT = mba_product_spider

class Scraper:
    def __init__(self, spider: ScrapySpider):
        # TODO. change working dir to dir crawler
        # TODO. appand sys path to dir crawler
        # change working directory to spider project root dir
        crawler_dir = "/".join(str(Path(__file__)).split("/")[0:-1])
        os.chdir(crawler_dir)
        sys.path.append(crawler_dir)
        settings_file_path = 'mba_crawler.settings' # The path seen from root, ie. from main.py
        os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
        self.process = CrawlerProcess(get_project_settings())
        self.spider = spider.value # The spider you want to crawl

    def run_spider(self, crawling_mba_request: CrawlingMBARequest, url_data_path=None):
        # init of spider
        self.process.crawl(self.spider, crawling_mba_request, url_data_path=url_data_path)
        # start_requests of spider
        self.process.start()  # the script will block here until the crawling is finished


if __name__ == "__main__":
    pass


############ deprecated

process = CrawlerProcess(settings={
    "FEEDS": {
        "items.json": {"format": "json"},
    },
})
# Tutorial : https://stackoverflow.com/questions/31662797/getting-scrapy-project-settings-when-script-is-outside-of-root-directory
def start_overview_spider(crawling_mba_overview, csv_path=""):
    """
    pod_product = "shirt"
    sort = "newest","best_seller"
    """

    #system(f'scrapy crawl mba_overview -a marketplace={crawling_mba_overview.marketplace} -a pod_product={crawling_mba_overview.pod_product} -a sort={crawling_mba_overview.sort} -a keyword={crawling_mba_overview.keyword} -a pages={crawling_mba_overview.pages} -a start_page={crawling_mba_overview.start_page} -a csv_path={csv_path}')
    process.crawl(mba_overview_spider, marketplace=crawling_mba_overview.marketplace, pod_product=crawling_mba_overview.pod_product, sort=crawling_mba_overview.sort, keyword=crawling_mba_overview.keyword, pages=crawling_mba_overview.pages, start_page=crawling_mba_overview.start_page, csv_path=csv_path)
    process.start() # the script will block here until the crawling is finished


def start_product_spider(marketplace, daily=True):
    process.crawl(mba_product_spider, marketplace=marketplace, daily=daily)
    process.start() # the script will block here until the crawling is finished

    #Scraper(ScrapySpider.OVERVIEW)
    #start_overview_spider("de", "shirt", "newest", keyword="", pages=1, start_page=1, csv_path="")