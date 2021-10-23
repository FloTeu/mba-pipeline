from datetime import time
from datetime import date, timedelta
try:
    from paapi5_python_sdk.api.default_api import DefaultApi
    from paapi5_python_sdk.models.condition import Condition
    from paapi5_python_sdk.models.get_items_request import GetItemsRequest
    from paapi5_python_sdk.models.get_items_resource import GetItemsResource
    from paapi5_python_sdk.models.partner_type import PartnerType
    from paapi5_python_sdk.rest import ApiException
except Exception as e:
    print("Error while importing paapi5",str(e))
#from amazon.paapi import AmazonAPI

import pandas as pd
import re
import time
from datetime import datetime
import sys
from pathlib import Path

file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append("..")
sys.path.append(str(root))

import create_url_csv
#from create_url_csv import get_asins_daily_to_crawl

# constants
RESOURCE_ASIN = "asin"
RESOURCE_TITLE = "title"
RESOURCE_BRAND = "brand"
RESOURCE_BRAND_URL = "brand_url"
RESOURCE_PRICE_CURRENCY = "price_currency"
RESOURCE_PRICE = "price"
RESOURCE_BSR = "bsr"
RESOURCE_BSR_CATEGORY = "bsr_category"
RESOURCE_BSR_NODES = "bsr_nodes"
RESOURCE_BSR_NODES_CATEGORIES = "bsr_nodes_categories"
RESOURCE_FIT_TYPES = "fit_types"
RESOURCE_COLOR_NAMES = "color_names"
RESOURCE_LISTINGS = "listings"
RESOURCE_DESCRIPTION = "description"
RESOURCE_UPLOAD_DATE = "upload_date"
RESOURCE_REVIEW_SCORE = "review_score"
RESOURCE_REVIEW_COUNT = "review_count"
RESOURCE_IMAGE_URLS = "image_urls"
RESOURCE_PRODUCT_TYPE = "product_type"
RESOURCE_AFFILIATE_URL = "affiliate_url"


EXCLUDE_ASINS = ["B00N3THBE8", "B076LTLG1Q", "B001EAQB12", "B001EAQB12", "B00OLG9GOK", "B07VPQHZHZ", "B076LX1H2V",
    "B0097B9SKQ", "B001EAQBH6", "B084X5Z1RX", "B07VPQHZHZ", "B07N4CHR77", "B002LBVRJO", "B00O1QQNGE",
    "B084ZRCLBD", "B084JBK66T", "B07VRY4WL3", "B078KR341N", "B00MP1PPHK", "B000YEVF4C", "B07WL5C9G9"
    ,"B07WVM8QBX", "B076LTN3ZV", "B016QM4XAI", "B007VATVL6", "B00U6U8GXC", "B00JZQHZ6C", "B00B69A928", "B0731RSZ8V"
    , "B01N2I5UO7", "B01MU11HZ4", "B00K5R9XCY", "B07BP9MDDR", "B0845C7JWN", "B0731RB39G", "B00Q4L52EI", "B0731R9KN4",
    "B084ZRG8T8", "B07W7F64J1", "B084WYWVDY", "B00PK2JBIA", "B07G5JXZZZ", "B07MVM8QBX", "B08P45JK6P", "B08P49MY6P", "B07G57GSW3",
    "B07SPXP8G4", "B00N3THB8E", "B01LZ3CICA", "B07V5P1VCP", "B0731RGXDP", "B076LWZHPC", "B0731T51WL", "B073D183X3",
    "B07NQ41MLR", "B0719BMPLY", "B083QNVF1P", "B076LX7HR2", "B083QNKLY5", "B083QNX4RM", "B07RJRXRPZ", "B07G5HX57H",
    "B07G57MJHF", "B0779HF6W1", "B002LBVQS6", "B014N6DPJY", "B003Q6CM8I", "B07VCTKYLH", "B07YZB46DM", "B0731RY1SM",
    "B08CJJ612P", "B08CCXZ62B"]
STRANGE_LAYOUT = ["B08P4P6NW2", "B08P9RSFPB", "B08P715HSQ", "B08P6ZZZYD", "B08NPN1BSM", "B08P6W8DF5", "B08P6Z741L", "B08NF2KRVD",
"B08P6YR7H1", "B08P745NZF", "B08P11VQT1", "B08P7254PL", "B08P6Y478X", "B08P4WF7BJ", "B08P4W854L", "B08P5WJN16", "B08P5BLGCG", "B08PB5H8MX",
"B08P9TJT15", "B08P96596Z", "B08P7DN9DK","B08P6S9BFW", "B08P6L9YNY", "B08P6Z6398", "B08P9HGNV1", "B08P94XH62", "B08P9T4DPT"
, "B08P761ZZ7", "B08P72GHH8", "B08PBPR798", "B08PBHYMTT", "B08NJMYW38", "B07X9H69QR","B08PGXQHHB", "B08PFJ28B3", "B08PGRSQKT",
"B08PM69M79", "B08PGX58MF", "B08PGL55LR"]

def parse_response(item_response_list):
    """
    The function parses GetItemsResponse and creates a dict of ASIN to Item object
    :param item_response_list: List of Items in GetItemsResponse
    :return: Dict of ASIN to Item object
    """
    mapped_response = {}
    for item in item_response_list:
        mapped_response[item.asin] = item
    return mapped_response

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

class MBAProducts(object):
    '''Class to access Amazon Product API
    '''
    def __init__(self, path_to_credentials_csv, partner_tag, marketplace="de"):
        self.cred_df = pd.read_csv(path_to_credentials_csv)
        self.access_key = self.cred_df["Access Key"].iloc[0]
        self.secret_key = self.cred_df["Secret Key"].iloc[0]
        self.partner_tag = partner_tag
        self.marketplace = marketplace

        if self.marketplace == "de":
            self.host = "webservices.amazon.de"
            self.region = "eu-west-1"
        elif self.marketplace == "com":
            self.host = "webservices.amazon.com"
            self.region = "us-east-1"
        else:
            raise NotImplementedError(f"Marketplace {self.marketplace} not definded")

        self.api = DefaultApi(
            access_key=self.access_key, secret_key=self.secret_key, host=self.host, region=self.region
        )

        # self.product_resources_to_get = [RESOURCE_TITLE, RESOURCE_BRAND, RESOURCE_BRAND_URL, RESOURCE_PRICE, RESOURCE_PRICE_CURRENCY,
        #  RESOURCE_BSR, RESOURCE_BSR_CATEGORY, RESOURCE_FIT_TYPES, RESOURCE_COLOR_NAMES, RESOURCE_LISTINGS, RESOURCE_PRODUCT_TYPE,
        #  RESOURCE_DESCRIPTION, RESOURCE_UPLOAD_DATE, RESOURCE_REVIEW_SCORE, RESOURCE_REVIEW_COUNT, RESOURCE_IMAGE_URLS,
        #  RESOURCE_AFFILIATE_URL, RESOURCE_ASIN, RESOURCE_BSR_NODES, RESOURCE_BSR_NODES_CATEGORIES]
        
        self.product_resources_to_get = [RESOURCE_TITLE, RESOURCE_BRAND, RESOURCE_PRICE, RESOURCE_PRICE_CURRENCY,
         RESOURCE_BSR, RESOURCE_BSR_CATEGORY, RESOURCE_LISTINGS, RESOURCE_IMAGE_URLS,
         RESOURCE_AFFILIATE_URL, RESOURCE_ASIN, RESOURCE_BSR_NODES, RESOURCE_BSR_NODES_CATEGORIES]
        
        self.set_items_resources()

    def set_items_resources(self):
        """ Choose resources you want from GetItemsResource enum """
        """ For more details, refer: https://webservices.amazon.com/paapi5/documentation/get-items.html#resources-parameter """

        # set all item resources for PAAPI
        self.items_resources = []
        if RESOURCE_TITLE in self.product_resources_to_get:
            self.items_resources.append(GetItemsResource.ITEMINFO_TITLE)
        if any(resource in self.product_resources_to_get for resource in [RESOURCE_PRICE_CURRENCY, RESOURCE_PRICE]):
            self.items_resources.append(GetItemsResource.OFFERS_LISTINGS_PRICE)
        if any(resource in self.product_resources_to_get for resource in [RESOURCE_REVIEW_COUNT, RESOURCE_REVIEW_SCORE]):
            self.items_resources.append(GetItemsResource.CUSTOMERREVIEWS_COUNT)
            self.items_resources.append(GetItemsResource.ITEMINFO_CONTENTRATING)
        if any(resource in self.product_resources_to_get for resource in [RESOURCE_BSR, RESOURCE_BSR_CATEGORY]):
            self.items_resources.append(GetItemsResource.BROWSENODEINFO_BROWSENODES_SALESRANK)
            self.items_resources.append(GetItemsResource.BROWSENODEINFO_WEBSITESALESRANK)
        if RESOURCE_IMAGE_URLS in self.product_resources_to_get:
            self.items_resources.append(GetItemsResource.IMAGES_PRIMARY_LARGE)
            self.items_resources.append(GetItemsResource.IMAGES_PRIMARY_MEDIUM)
            self.items_resources.append(GetItemsResource.IMAGES_PRIMARY_SMALL)
        if RESOURCE_LISTINGS in self.product_resources_to_get:
            self.items_resources.append(GetItemsResource.ITEMINFO_FEATURES)
        if RESOURCE_UPLOAD_DATE in self.product_resources_to_get:
            self.items_resources.append(GetItemsResource.ITEMINFO_CONTENTINFO)
        if any(resource in self.product_resources_to_get for resource in [RESOURCE_BRAND, RESOURCE_BRAND_URL]):
            self.items_resources.append(GetItemsResource.ITEMINFO_BYLINEINFO)

        self.items_resources.append(GetItemsResource.ITEMINFO_PRODUCTINFO)
        #self.items_resources.append(GetItemsResource.ITEMINFO_MANUFACTUREINFO)

    def get_items_resources(self):
        return self.items_resources

    def extract_resource(self, resource_name, res_dict):
        try:
            if resource_name == RESOURCE_BRAND:
                return res_dict.item_info.by_line_info.brand.display_value
            if resource_name == RESOURCE_ASIN:
                return res_dict.asin
            elif resource_name == RESOURCE_BSR:
                return res_dict.browse_node_info.website_sales_rank.sales_rank
            elif resource_name == RESOURCE_BSR_CATEGORY:
                return res_dict.browse_node_info.website_sales_rank.context_free_name
            elif resource_name == RESOURCE_BSR_NODES:
                return [v.sales_rank for v in res_dict.browse_node_info.browse_nodes]
            elif resource_name == RESOURCE_BSR_NODES_CATEGORIES:
                return [v.context_free_name for v in res_dict.browse_node_info.browse_nodes]
            # elif resource_name == RESOURCE_COLOR_NAMES:
            #     pass
            # elif resource_name == RESOURCE_DESCRIPTION:
            #     pass
            # elif resource_name == RESOURCE_FIT_TYPES:
            #     pass
            elif resource_name == RESOURCE_IMAGE_URLS:
                return res_dict.images.primary.large.url
            elif resource_name == RESOURCE_LISTINGS:
                return res_dict.item_info.features.display_values
            elif resource_name == RESOURCE_PRICE:
                return res_dict.offers.listings[0].price.amount
            elif resource_name == RESOURCE_PRICE_CURRENCY:
                return res_dict.offers.listings[0].price.currency
            # elif resource_name == RESOURCE_REVIEW_COUNT:
            #     pass
            # elif resource_name == RESOURCE_REVIEW_SCORE:
            #     pass
            elif resource_name == RESOURCE_UPLOAD_DATE:
                return res_dict.item_info.content_info.publication_date.display_value
            elif resource_name == RESOURCE_TITLE:
                return res_dict.item_info.title.display_value
            #elif resource_name == RESOURCE_PRODUCT_TYPE:
                #return res_dict.browse_node_info.browse_nodes.large.url
            elif resource_name == RESOURCE_AFFILIATE_URL:
                return res_dict.detail_page_url
            else:
                print(f"Resource {resource_name} is not defined to extract data from response")
                return None
        except Exception as e:
            #print(f"Something went wrong while trying to get data of resource {resource_name}", str(e))
            return None
        
    def response2MWData(self, response_dict):
        mw_data = []
        for asin, res_dict in response_dict.items():
            mw_data_item = {}
            for resource_name in self.product_resources_to_get:
                mw_data_item.update({resource_name: self.extract_resource(resource_name, res_dict)})
            mw_data.append(mw_data_item)
        return mw_data

    def get_mw_data(self, asin_list):
        mw_data = []
        try:
            get_items_request = GetItemsRequest(
                partner_tag=self.partner_tag,
                partner_type=PartnerType.ASSOCIATES,
                marketplace=f"www.amazon.{self.marketplace}",
                condition=Condition.NEW,
                item_ids=asin_list,
                resources=self.get_items_resources(),
            )
        except ValueError as exception:
            print("Error in forming GetItemsRequest: ", exception)
            return mw_data

        try:
            """ Sending request """
            response = self.api.get_items(get_items_request)
            #print("API called Successfully")
            if response.errors is not None:
                try:
                    #print("Found errors/ ASINS have takedows")
                    for error in response.errors:
                        asin = error.message.split("ItemId")[1:2][0].strip().split(" ")[0]
                        mw_data_takedown = {RESOURCE_ASIN: asin, RESOURCE_BSR: 404, RESOURCE_BSR_CATEGORY: "404", RESOURCE_BSR_NODES: [404], RESOURCE_BSR_NODES_CATEGORIES: ["404"], RESOURCE_PRICE: 404.0, RESOURCE_PRICE_CURRENCY: "404"}
                        mw_data.append(mw_data_takedown)
                except Exception as e:
                    print("Could not append mw_data with takedown asins",str(e))

            """ Parse response """
            if response.items_result is not None:
                response_dict = parse_response(response.items_result.items)
                mw_data.extend(self.response2MWData(response_dict))
                return mw_data
            else:
                print("Could not get item result of api response")
                return mw_data

        except ApiException as exception:
            print("Error calling PA-API 5.0!")
            print("Status code:", exception.status)
            print("Errors :", exception.body)
            print("Request ID:", exception.headers["x-amzn-RequestId"])
            return mw_data
        except Exception as exception:
            print("Exception :", exception)
            return mw_data


    # def get_items(self, asin_list):
    #     """ Following are your credentials """
    #     """ Please add your access key here """

    #     """ PAAPI host and region to which you want to send request """
    #     """ For more details refer: https://webservices.amazon.com/paapi5/documentation/common-request-parameters.html#host-and-region"""


    #     """ API declaration """
    #     default_api = DefaultApi(
    #         access_key=self.access_key, secret_key=self.secret_key, host=self.host, region=self.region
    #     )

    #     """ Request initialization"""

    #     """ Forming request """

    #     try:
    #         get_items_request = GetItemsRequest(
    #             partner_tag=self.partner_tag,
    #             partner_type=PartnerType.ASSOCIATES,
    #             marketplace=f"www.amazon.{self.marketplace}",
    #             condition=Condition.NEW,
    #             item_ids=asin_list,
    #             resources=self.get_items_resources(),
    #         )
    #     except ValueError as exception:
    #         print("Error in forming GetItemsRequest: ", exception)
    #         return

    #     try:
    #         """ Sending request """
    #         response = default_api.get_items(get_items_request)

    #         print("API called Successfully")
    #         print("Complete Response:", response)

    #         """ Parse response """
    #         if response.items_result is not None:
    #             print("Printing all item information in ItemsResult:")
    #             response_dict = parse_response(response.items_result.items)
    #             return self.response2MWData(response_dict)
    #     except ApiException as exception:
    #         print("Error calling PA-API 5.0!")
    #         print("Status code:", exception.status)
    #         print("Errors :", exception.body)
    #         print("Request ID:", exception.headers["x-amzn-RequestId"])

    #     #         for item_id in asin_list:
    #     #             print("Printing information about the item_id: ", item_id)
    #     #             if item_id in response_list:
    #     #                 item = response_list[item_id]
    #     #                 if item is not None:
    #     #                     if item.asin is not None:
    #     #                         print("ASIN: ", item.asin)
    #     #                     if item.detail_page_url is not None:
    #     #                         print("DetailPageURL: ", item.detail_page_url)
    #     #                     if (
    #     #                         item.item_info is not None
    #     #                         and item.item_info.title is not None
    #     #                         and item.item_info.title.display_value is not None
    #     #                     ):
    #     #                         print("Title: ", item.item_info.title.display_value)
    #     #                     if (
    #     #                         item.offers is not None
    #     #                         and item.offers.listings is not None
    #     #                         and item.offers.listings[0].price is not None
    #     #                         and item.offers.listings[0].price.display_amount is not None
    #     #                     ):
    #     #                         print(
    #     #                             "Buying Price: ",
    #     #                             item.offers.listings[0].price.display_amount,
    #     #                         )
    #     #             else:
    #     #                 print("Item not found, check errors")

    #     #     if response.errors is not None:
    #     #         print("\nPrinting Errors:\nPrinting First Error Object from list of Errors")
    #     #         print("Error code", response.errors[0].code)
    #     #         print("Error message", response.errors[0].message)

    #     # except ApiException as exception:
    #     #     print("Error calling PA-API 5.0!")
    #     #     print("Status code:", exception.status)
    #     #     print("Errors :", exception.body)
    #     #     print("Request ID:", exception.headers["x-amzn-RequestId"])

    #     except TypeError as exception:
    #         print("TypeError :", exception)
    #     except ValueError as exception:
    #         print("ValueError :", exception)
    #     except Exception as exception:
    #         print("Exception :", exception)


class MBADataUpdater(object):
    '''Class Update BQ Tables with MBA API data
    '''
    def __init__(self, mbaProduct, max_requests=8000, project_id="mba-pipeline", requests_per_second=1, max_parallel_item_ids=10):
        self.mbaProduct = mbaProduct
        self.marketplace = self.mbaProduct.marketplace
        self.project_id = project_id
        self.asins_to_update = []
        self.max_requests = max_requests
        self.max_parallel_item_ids = max_parallel_item_ids
        self.max_asins_to_request = max_requests * max_parallel_item_ids
        self.requests_per_second = requests_per_second
        self.seconds_sleep_between_requests = 1 / requests_per_second
        self.bq_daily_table_id = f"mba_{self.marketplace}.products_details_daily_api"
        self.bq_mba_images_table_id = f"mba_{self.marketplace}.products_mba_images"

        # asins which should not be updated        
        self.exclude_asins = EXCLUDE_ASINS + STRANGE_LAYOUT

    def set_asins_to_update(self, proportions=[0.7,0.2,0.1], file_path=None):
        if file_path:
            df_tocrawl = pd.read_csv(file_path)
        else:
            df_tocrawl = create_url_csv.get_asins_daily_to_crawl(self.marketplace, self.exclude_asins, self.max_asins_to_request, proportions=proportions)
            # make sure max requests are not outreached
        df_tocrawl = df_tocrawl.iloc[0:self.max_asins_to_request]
        self.asins_to_update = df_tocrawl["asin"].to_list()

    def update_daily_table(self, batch_size=1000):
        batch_count = 1
        batches = int(len(self.asins_to_update) / batch_size) + 1
        for update_batch in batch(self.asins_to_update, n=batch_size):
            print(f"Batch {batch_count} of {batches}")
            df_update_batch = pd.DataFrame()
            for api_batch in batch(update_batch, n=self.max_parallel_item_ids):
                time.sleep(self.seconds_sleep_between_requests)
                mw_data = self.mbaProduct.get_mw_data(api_batch)
                if len(mw_data) == 0:
                    print("Max Request Limit reached!")
                    break
                df_api_batch = pd.DataFrame(mw_data)
                df_update_batch = df_update_batch.append(df_api_batch)
            df_update_batch_clean = df_update_batch[[RESOURCE_ASIN, RESOURCE_BSR, RESOURCE_BSR_CATEGORY, RESOURCE_BSR_NODES, RESOURCE_BSR_NODES_CATEGORIES, RESOURCE_PRICE, RESOURCE_PRICE_CURRENCY]]
            df_update_batch_clean[RESOURCE_BSR] = df_update_batch_clean[RESOURCE_BSR].astype('Int64')
            df_update_batch_clean["timestamp"] = datetime.now()
            df_update_batch_clean.to_gbq(self.bq_daily_table_id, if_exists="append", project_id=self.project_id)
            if len(mw_data) == 0:
                break
            batch_count = batch_count + 1

    def update_mba_images_table(self, batch_size=1000):
        batch_count = 1
        batches = int(len(self.asins_to_update) / batch_size) + 1
        for update_batch in batch(self.asins_to_update, n=batch_size):
            print(f"Batch {batch_count} of {batches}")
            df_update_batch = pd.DataFrame()
            for api_batch in batch(update_batch, n=self.max_parallel_item_ids):
                time.sleep(self.seconds_sleep_between_requests)
                mw_data = self.mbaProduct.get_mw_data(api_batch)
                if len(mw_data) == 0:
                    print("Max Request Limit reached!")
                    break
                df_api_batch = pd.DataFrame(mw_data)
                df_update_batch = df_update_batch.append(df_api_batch)
            df_update_batch_clean = df_update_batch[[RESOURCE_ASIN, RESOURCE_IMAGE_URLS]]
            df_update_batch_clean["url_image_lowq"] = df_update_batch_clean[RESOURCE_IMAGE_URLS]
            df_update_batch_clean["url_image_q2"] = df_update_batch_clean[RESOURCE_IMAGE_URLS]
            df_update_batch_clean["url_image_q3"] = df_update_batch_clean[RESOURCE_IMAGE_URLS]
            df_update_batch_clean["url_image_q4"] = df_update_batch_clean[RESOURCE_IMAGE_URLS]
            df_update_batch_clean["url_image_hq"] = df_update_batch_clean[RESOURCE_IMAGE_URLS]
            df_update_batch_clean["timestamp"] = datetime.now()
            df_update_batch_clean = df_update_batch_clean[~df_update_batch_clean["url_image_lowq"].isna()]
            df_update_batch_clean = df_update_batch_clean[[RESOURCE_ASIN, "url_image_lowq", "url_image_q2", "url_image_q3", "url_image_q4", "url_image_hq", "timestamp"]]
            df_update_batch_clean.to_gbq(self.bq_mba_images_table_id, if_exists="append", project_id=self.project_id)
            
            # update daily data
            df_update_batch__daily_clean = df_update_batch[[RESOURCE_ASIN, RESOURCE_BSR, RESOURCE_BSR_CATEGORY, RESOURCE_BSR_NODES, RESOURCE_BSR_NODES_CATEGORIES, RESOURCE_PRICE, RESOURCE_PRICE_CURRENCY]]
            df_update_batch__daily_clean[RESOURCE_BSR] = df_update_batch__daily_clean[RESOURCE_BSR].astype('Int64')
            df_update_batch__daily_clean["timestamp"] = datetime.now()
            df_update_batch__daily_clean.to_gbq(self.bq_daily_table_id, if_exists="append", project_id=self.project_id)

            if len(mw_data) == 0:
                break
            batch_count = batch_count + 1

if __name__ == '__main__':
    mbaProduct = MBAProducts("/home/f_teutsch/PAAPICredentials.csv", "merchwatch0f-21", marketplace="de")
    # make request to prevent limit reached error throwed every first time api is called
    mbaProduct.get_mw_data(["B096FGPG6C"])
    mbaUpdater = MBADataUpdater(mbaProduct, max_requests=4000)
    mbaUpdater.set_asins_to_update(proportions=[0.1,0.5,0.4])#, file_path="~/no_images.csv")
    #mbaUpdater.update_mba_images_table()
    mbaUpdater.update_daily_table()
    #mw_data = mbaProduct.get_mw_data(["B08NWKWLYZ","B07K9SS7L2", "B086DFZBRM", "B0868557JQ"])
    
