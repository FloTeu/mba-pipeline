
import json
import pathlib
from enum import Enum
from mwfunctions.pydantic.base_classes import EnumBase

class CrawlingJsonType(str, EnumBase):
    DAILY="daily"
    GENERAL="general"
    OVERVIEW_WEEK="overview_week"
    OVERVIEW_SUNDAY="overview_sunday"

CRAWLING_TYPES = ["daily", "general"]


def get_crawling_request_data(crawling_type: CrawlingJsonType):
    assert crawling_type in CrawlingJsonType.to_list(), f"crawl_type must be one of '{CrawlingJsonType.to_list()}'"
    dir_path = pathlib.Path(__file__).parent.resolve()
    if crawling_type == CrawlingJsonType.DAILY:
        with open(f'{dir_path}/data_product_daily.json') as json_file:
            data = json.load(json_file)
    elif crawling_type == CrawlingJsonType.GENERAL:
        with open(f'{dir_path}/data_product_general.json') as json_file:
            data = json.load(json_file)
    elif crawling_type == CrawlingJsonType.OVERVIEW_WEEK:
        with open(f'{dir_path}/data_overview_week.json') as json_file:
        #with open(f'{dir_path}/data_overview_test.json') as json_file:
            data = json.load(json_file)
    elif crawling_type == CrawlingJsonType.OVERVIEW_SUNDAY:
        with open(f'{dir_path}/data_overview_sunday.json') as json_file:
            data = json.load(json_file)
    else:
        raise NotImplementedError

    return data



