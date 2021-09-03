import scrapy
from scrapy.crawler import CrawlerProcess
from mwfunctions.crawler.scrapy.mba_crawler.spiders.mba_overview_spider import MBASpider as mba_overview_spider
class MySpider(scrapy.Spider):
    # Your spider definition
    ...

process = CrawlerProcess(settings={
    "FEEDS": {
        "items.json": {"format": "json"},
    },
})

process.crawl(mba_overview_spider, marketplace="de", pod_product='shirt', sort="newest", pages=1)
process.start() # the script will block here until the crawling is finished
test = 0