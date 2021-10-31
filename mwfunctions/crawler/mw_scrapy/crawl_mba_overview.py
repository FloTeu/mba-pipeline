# #!/bin/sh
# cd
# home /
# rm - rf
# mba - pipeline /
# git
# clone
# https: // github.com / Flo95x / mba - pipeline.git
# sudo
# pip3
# install - r / home / mba - pipeline / crawler / mba / requirements.txt
# cd
# mba - pipeline
# sudo
# pip3
# install - e.
# sudo
# pip3
# install
# google - cloud - logging
# yes | sudo
# apt - get
# install
# python - setuptools
# sudo
# python3
# setup.py
# build
# sudo
# python3
# setup.py
# install
# sudo
# pip3
# install - e.
# cd
# crawler / mba /
# # sudo git pull
# sudo
# mkdir
# data
# sudo
# mkdir
# data / shirts
# sudo
# chmod
# 777
# data /
# sudo
# chmod
# 777
# data / shirts
# cd
# mba_crawler
# sudo
# mkdir
# proxy
# cd
# proxy
# sudo
# cp / home / flo_t_1995 / proxies.json.
# sudo
# cp / home / flo_t_1995 / proxy_handler.py.
# sudo
# cp / home / flo_t_1995 / utils.py.
# cd..
# sudo
# python3
# change_spider_settings.py
# de
# sudo
# scrapy
# crawl
# mba_overview - a
# marketplace = de - a
# pod_product = shirt - a
# sort = best_seller - a
# pages = 100 - a
# start_page = 1
# sudo
# python3
# change_spider_settings.py
# com
# sudo
# scrapy
# crawl
# mba_overview - a
# marketplace = com - a
# pod_product = shirt - a
# sort = best_seller - a
# pages = 50 - a
# start_page = 1
#
# sudo
# python3
# change_spider_settings.py
# de
# sudo
# scrapy
# crawl
# mba_overview - a
# marketplace = de - a
# pod_product = shirt - a
# sort = best_seller - a
# pages = 100 - a
# start_page = 300
# sudo
# python3
# change_spider_settings.py
# com
# sudo
# scrapy
# crawl
# mba_overview - a
# marketplace = com - a
# pod_product = shirt - a
# sort = best_seller - a
# pages = 50 - a
# start_page = 300
#
# sudo
# python3
# change_spider_settings.py
# de
# sudo
# scrapy
# crawl
# mba_overview - a
# marketplace = de - a
# pod_product = shirt - a
# sort = newest - a
# pages = 10 - a
# start_page = 1
# sudo
# python3
# change_spider_settings.py
# com
# sudo
# scrapy
# crawl
# mba_overview - a
# marketplace = com - a
# pod_product = shirt - a
# sort = newest - a
# pages = 10 - a
# start_page = 1


from mwfunctions.pydantic.crawling_classes import CrawlingMBAOverviewRequest, CrawlingMBAProductRequest, CrawlingMBARequest
from mwfunctions.crawler.mw_scrapy import Scraper, ScrapyMBASpider

if __name__ == '__main__':
    crawling_mba_overview_request = CrawlingMBAOverviewRequest(marketplace=marketplace, sort="newest",
                                                             pages=0, start_page=1)
    crawling_mba_overview_request.reset_crawling_job_id()

    scraper = Scraper(ScrapyMBASpider.OVERVIEW)
    scraper.run_spider(crawling_mba_overview_request, wait_until_finished=True)