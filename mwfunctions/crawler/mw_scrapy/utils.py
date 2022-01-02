import os
import pandas as pd
from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingInputItem
from mwfunctions.crawler.preprocessing import create_url_csv

from typing import Union, List, Dict


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

def get_overview_data_sql(marketplace, asins, project_id="mba-pipeline"):
    asins_str = "'" + "','".join(asins) + "'"
    WHERE_STATEMENT = f"WHERE t0.asin IN ({asins_str})"
    return """
        SELECT t0.*, CAST(REPLACE(t1.price, ',', '.') as FLOAT64) as price_overview
        FROM `{1}.mba_{0}.products_mba_images` t0
        LEFT JOIN `{1}.mba_{0}.products` t1 on t1.asin = t0.asin
        {2}
        ORDER BY t0.timestamp desc
        """.format(marketplace, project_id, WHERE_STATEMENT)

def get_asin2overview_data_dict(mba_product_request, marketplace, asins: List[str]=None, url_data_path=None, debug=False) -> Dict[str, dict]:
    # get crawling input from csv file
    asin2overview_data_dict = {}
    url_data_path = url_data_path if url_data_path else mba_product_request.url_data_path
    assert asins or url_data_path or mba_product_request.url_data_path, "Either list of asins must be provided or url_data_path"
    if url_data_path:
        df = pd.read_csv(url_data_path, engine="python")
        # only create dict if it contains overview data like price info
        asin2overview_data_dict = df.set_index('asin').to_dict("index") if "price_overview" in df.columns.values else {}
    # if not already set try to get overview data with BQ
    if asin2overview_data_dict == {}:
        project_id = "merchwatch-dev" if debug else "mba-pipeline"
        df = pd.read_gbq(get_overview_data_sql(marketplace, asins, project_id=project_id), project_id=project_id)
        df = df.drop_duplicates(["asin"],keep="first")
        asin2overview_data_dict = df.set_index('asin').to_dict("index")

    return asin2overview_data_dict


