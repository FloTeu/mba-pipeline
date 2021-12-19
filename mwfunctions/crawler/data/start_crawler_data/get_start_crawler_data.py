
import json
import pathlib
from enum import Enum
from mwfunctions.pydantic.base_classes import EnumBase

class CrawlingJsonType(str, EnumBase):
    # values of enum must identify json file
    DAILY="product_daily"
    GENERAL="product_general"
    OVERVIEW_WEEK_NEWEST="overview_week_newest"
    OVERVIEW_WEEK_BESTSELLER_FIRST="overview_week_bestseller_first"
    OVERVIEW_WEEK_BESTSELLER_SECOND="overview_week_bestseller_second"
    OVERVIEW_SUNDAY_NEWEST="overview_sunday_newest"
    OVERVIEW_SUNDAY_BESTSELLER="overview_sunday_bestseller"

def get_crawling_request_data(crawling_type: CrawlingJsonType):
    assert crawling_type in CrawlingJsonType.to_list(), f"crawl_type must be one of '{CrawlingJsonType.to_list()}'"
    dir_path = pathlib.Path(__file__).parent.resolve()
    with open(f'{dir_path}/data_{crawling_type}.json') as json_file:
        data = json.load(json_file)
    return data

def set_crawling_request_data(crawling_type: CrawlingJsonType, data_dict):
    assert crawling_type in CrawlingJsonType.to_list(), f"crawl_type must be one of '{CrawlingJsonType.to_list()}'"
    dir_path = pathlib.Path(__file__).parent.resolve()
    with open(f'{dir_path}/data_{crawling_type}.json', 'w') as fp:
        json.dump(data_dict, fp)


