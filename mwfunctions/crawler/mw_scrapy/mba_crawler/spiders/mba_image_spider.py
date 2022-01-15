import scrapy

from mwfunctions.logger import get_logger
from mwfunctions import environment
from mwfunctions.crawler.proxy.utils import get_random_headers
from mwfunctions.crawler.mw_scrapy.base_classes.spider_base import MBASpider
from mwfunctions.pydantic.crawling_classes import CrawlingMBAImageRequest, CrawlingType

environment.set_cloud_logging()
LOGGER = get_logger(__name__, labels_dict={"topic": "crawling", "target": "image", "type": "scrapy"}, do_cloud_logging=True)



class MBAImageSpider(MBASpider):
    name = "mba_image"
    website_crawling_target = CrawlingType.IMAGE.value

    def __init__(self, mba_image_request: CrawlingMBAImageRequest, *args, **kwargs):
        super_attrs = {"mba_crawling_request": mba_image_request, **mba_image_request.dict()}
        super(MBAImageSpider, self).__init__(*args, **super_attrs)
        self.mba_image_request: CrawlingMBAImageRequest = mba_image_request

    def start_requests(self):
        test = 0
        headers = get_random_headers(self.marketplace)
        yield scrapy.Request(url=self.mba_image_request.mba_image_items.image_items[0].url, callback=self.parse, headers=headers, priority=0,
                                    errback=self.errback_httpbin)

    def parse(self, response, **kwargs):
        yield {"pydantic_class": self.mba_image_request.mba_image_items}