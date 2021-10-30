import argparse
import json
import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from mwfunctions.pydantic.crawling_classes import CrawlingType
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_overview_spider import MBAShirtOverviewSpider as mba_overview_spider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_product_general_spider import MBALocalProductSpider as mba_product_spider
from mwfunctions.crawler.mw_scrapy.tests import TestingSpider
from mwfunctions.pydantic.crawling_classes import CrawlingMBARequest, CrawlingMBAOverviewRequest, CrawlingMBAProductRequest

def main(crawling_data_class_file_path, crawling_type, **kwargs):
    with open(crawling_data_class_file_path) as json_file:
        data_class_dict = json.load(json_file)

    if crawling_type == CrawlingType.OVERVIEW.name:
        spider = mba_overview_spider
        crawling_mba_request: CrawlingMBARequest = CrawlingMBAOverviewRequest.parse_obj(data_class_dict)
    elif crawling_type == CrawlingType.PRODUCT.name:
        spider = mba_product_spider
        crawling_mba_request: CrawlingMBARequest = CrawlingMBAProductRequest.parse_obj(data_class_dict)
    else:
        raise NotImplementedError

    # parse arguments using optparse or argparse or what have you
    print(crawling_mba_request)
    process = CrawlerProcess(get_project_settings())
    process.crawl(spider, crawling_mba_request)
    process.start()
    test = 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start a mba crawler')
    parser.add_argument('crawling_type', help="Type of crawliing which is defined in Enum CrawlingType")
    parser.add_argument('crawling_data_class_file_path', help='File path to json file which can be transformed to crawling data class')

    args = parser.parse_args()

    main(args.crawling_data_class_file_path, args.crawling_type)
