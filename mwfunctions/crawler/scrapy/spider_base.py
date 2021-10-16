import scrapy

class MBASpider(scrapy.Spider):

    custom_settings = {
        # Set by settings.py
        # "ROTATING_PROXY_LIST": proxy_handler.get_http_proxy_list(only_usa=False),

        'ITEM_PIPELINES': {
            'mba_crawler.pipelines.MbaCrawlerItemPipeline': 100,
            'mba_crawler.pipelines.MbaCrawlerImagePipeline': 200
        },
    }

    def __init__(self, marketplace, debug=True, *args, **kwargs):
        self.marketplace = marketplace
        self.debug = debug
        super().__init__(**kwargs)  # python3


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
            Code to add custom settings AFTER init of spider classes
        """
        # init scrapy spider classes (child of MBASpider)
        spider = cls(*args, **kwargs)

        # Update setting with custom_settings
        crawler.settings.frozen = False
        crawler.settings.update(spider.custom_settings)
        crawler.settings.freeze()

        # set crawler to spider
        spider._set_crawler(crawler)
        return spider

class MBAShirtSpider(MBASpider):

    def __init__(self, marketplace, *args, **kwargs):
        super(MBAShirtSpider, self).__init__(marketplace, *args, **kwargs)

        self.custom_settings.update({
            'IMAGES_STORE': f'gs://5c0ae2727a254b608a4ee55a15a05fb7{"-debug" if self.debug else ""}/mba-shirts/',
            'GCS_PROJECT_ID': 'mba-pipeline' # google project of storage
            })


