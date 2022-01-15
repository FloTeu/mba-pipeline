import argparse
import json
import os
import sys

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from mwfunctions.pydantic.crawling_classes import CrawlingType
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_overview_spider import MBAShirtOverviewSpider as mba_overview_spider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_product_general_spider import MBALocalProductSpider as mba_product_spider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_realtime_spider import MBAShirtRealtimeResearchSpider as mba_realtime_spider
from mwfunctions.crawler.mw_scrapy.mba_crawler.spiders.mba_image_spider import MBAImageSpider as mba_image_spider
from mwfunctions.pydantic.crawling_classes import CrawlingMBARequest, CrawlingMBAOverviewRequest, CrawlingMBAProductRequest, CrawlingMBAImageRequest
from mwfunctions.image.conversion import b64_str2dict

def main(crawling_type, data_class_dict, **kwargs):
    #with open(crawling_data_class_file_path) as json_file:
    #data_class_dict = json.loads(crawling_data_class_json_str.replace('\\',''))

    if crawling_type == CrawlingType.OVERVIEW.name:
        spider = mba_overview_spider
        crawling_mba_request: CrawlingMBARequest = CrawlingMBAOverviewRequest.parse_obj(data_class_dict)
    elif crawling_type == CrawlingType.PRODUCT.name:
        spider = mba_product_spider
        crawling_mba_request: CrawlingMBARequest = CrawlingMBAProductRequest.parse_obj(data_class_dict)
    elif crawling_type == CrawlingType.REALTIME_RESEARCH.name:
        spider = mba_realtime_spider
        crawling_mba_request: CrawlingMBARequest = CrawlingMBAOverviewRequest.parse_obj(data_class_dict)
    elif crawling_type == CrawlingType.IMAGE.name:
        spider = mba_image_spider
        crawling_mba_request: CrawlingMBARequest = CrawlingMBAImageRequest.parse_obj(data_class_dict)
    else:
        raise NotImplementedError

    # parse arguments using optparse or argparse or what have you
    print(crawling_mba_request)
    process = CrawlerProcess(get_project_settings())
    # change settings
    for setting_name, setting_value in crawling_mba_request.settings.dict().items():
        process.settings.set(setting_name, setting_value, priority='cmdline')
    # Why was crawler already started after process.crawl?
    # Solution: env variable GOOGLE_CLOUD_PROJECT was not a merchwatch project
    process.crawl(spider, crawling_mba_request)
    process.start()
    test = 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start a mba crawler')
    parser.add_argument('crawling_type', help="Type of crawliing which is defined in Enum CrawlingType")
    #parser.add_argument('crawling_data_class_file_path', help='File path to json file which can be transformed to crawling data class')
    #parser.add_argument('-i', '--crawling_data_class_json_str', help='File path to json file which can be transformed to crawling data class', required=True)
    parser.add_argument('crawling_data_class_dict_b64_str', help='File path to json file which can be transformed to crawling data class')

    print("TRY TO GET DATA")
    #argv = sys.argv
    #crawling_type = sys.argv[1]
    #crawling_data_class_json_str = " ".join(sys.argv[2:len(argv)])
    #crawling_data_class_json_str = crawling_data_class_json_str.replace("'",'"')
    args = parser.parse_args()

    crawling_data_class_dict = b64_str2dict(args.crawling_data_class_dict_b64_str)
    print(args.crawling_type, crawling_data_class_dict)
    main(args.crawling_type, crawling_data_class_dict)
