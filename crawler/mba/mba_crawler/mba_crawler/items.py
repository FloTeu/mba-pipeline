# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MbaCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    image_urls = scrapy.Field()
    marketplace = scrapy.Field()
    asins = scrapy.Field()
    url_mba_lowqs = scrapy.Field()
