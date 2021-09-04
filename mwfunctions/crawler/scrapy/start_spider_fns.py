from scrapy.crawler import CrawlerProcess
from mwfunctions.crawler.scrapy.mba_crawler.spiders.mba_overview_spider import MBASpider as mba_overview_spider
from mwfunctions.crawler.scrapy.mba_crawler.spiders.mba_product_general_spider import MBASpider as mba_product_spider


process = CrawlerProcess(settings={
    "FEEDS": {
        "items.json": {"format": "json"},
    },
})

def start_overview_spider(marketplace, pod_product, sort, keyword="", pages=0, start_page=1, csv_path=""):
    """
    pod_product = "shirt"
    sort = "newest","best_seller"
    """
    process.crawl(mba_overview_spider, marketplace=marketplace, pod_product=pod_product, sort=sort, keyword=keyword, pages=pages, start_page=start_page, csv_path=csv_path)
    process.start() # the script will block here until the crawling is finished


def start_product_spider(marketplace, daily=True):
    process.crawl(mba_product_spider, marketplace=marketplace, daily=daily)
    process.start() # the script will block here until the crawling is finished
