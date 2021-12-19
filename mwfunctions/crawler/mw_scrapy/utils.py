import os
import pandas as pd
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingInputItem
from mwfunctions.crawler.preprocessing import create_url_csv

from typing import Union, List


def get_urls_asins_for_product_crawling(mba_product_request: CrawlingMBAProductRequest, marketplace, bq_project_id, url_data_path=None, debug=False):
    # get crawling input from csv file
    url_data_path = url_data_path if url_data_path else mba_product_request.url_data_path
    if url_data_path:
        urls = pd.read_csv(url_data_path, engine="python")["url"].tolist()
        asins = pd.read_csv(url_data_path)["asin"].tolist()
    # get crawling input from provided asins
    elif mba_product_request.asins_to_crawl:
        asins = mba_product_request.asins_to_crawl
        urls = [CrawlingInputItem(asin=asin, marketplace=marketplace).url for asin in asins]
    # get crawling input from BQ
    else:
        crawling_input_items: List[CrawlingInputItem] = create_url_csv.get_crawling_input_items(mba_product_request, bq_project_id=bq_project_id, progress_bar_type="tqdm" if debug else None)
        urls = [crawling_input_item.url for crawling_input_item in crawling_input_items]
        asins = [crawling_input_item.asin for crawling_input_item in crawling_input_items]

    return urls, asins