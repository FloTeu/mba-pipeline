
import json
import pathlib
CRAWLING_TYPES = ["daily", "general"]


def get_crawling_request_data(crawling_type):
    assert crawling_type in CRAWLING_TYPES, f"crawl_type must be one of '{CRAWLING_TYPES}'"
    dir_path = pathlib.Path(__file__).parent.resolve()
    if crawling_type == "daily":
        with open(f'{dir_path}/data_product_daily.json') as json_file:
            data = json.load(json_file)
    else:
        with open(f'{dir_path}/data_product_general.json') as json_file:
            data = json.load(json_file)

    return data



