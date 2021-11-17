from paapi5_python_sdk.api.default_api import DefaultApi
from paapi5_python_sdk.search_items_request import SearchItemsRequest
from paapi5_python_sdk.get_items_request import GetItemsRequest
from paapi5_python_sdk.get_items_resource import GetItemsResource
from paapi5_python_sdk.search_items_request import SearchItemsRequest
from paapi5_python_sdk.partner_type import PartnerType
from paapi5_python_sdk.rest import ApiException
from paapi5_python_sdk.sort_by import SortBy  # noqa: F401,E501


import json
import pandas as pd
import csv

from pydantic import BaseModel, Field
from typing import Optional

class AmazonPaApiCredentials(BaseModel):
    AccessKey: str = Field(description="Access key of amazon Product Advertising API. Your Access Key which uniquely identifies you.")
    SecretKey: str = Field(description="Secret key of amazon Product Advertising API. A key that is used in conjunction with the Access Key to cryptographically sign an API request. ")
    PartnerTag: Optional[str] = Field(None, description="Partner tag of amazon affiliate partner programm")

    def __init__(self, file_path=None, sep=",", *args, **kwargs):
        if file_path:
            if "json" in file_path:
                with open(file_path) as json_file:
                    data = json.load(json_file)
            if "csv" in file_path:
                with open(file_path, newline='') as csvfile:
                    reader = csv.DictReader(csvfile)
                    data = next(reader)
            super(AmazonPaApiCredentials, self).__init__(*args, **{**data,**kwargs})
        else:
            super(AmazonPaApiCredentials, self).__init__(*args, **kwargs)

    def get_api_client(self, marketplace="de"):
        # TODO: This works only for "de" marketplace
        if marketplace == "de":
            return DefaultApi(
                access_key=self.AccessKey, secret_key=self.SecretKey, host="webservices.amazon.de", region="eu-west-1"
            )
        else:
            raise NotImplementedError

def get_items_resources():
    """ Choose resources you want from GetItemsResource enum """
    """ For more details, refer: https://webservices.amazon.com/paapi5/documentation/get-items.html#resources-parameter """

    # set all item resources for PAAPI
    items_resources = []
    items_resources.append(GetItemsResource.ITEMINFO_TITLE)
    items_resources.append(GetItemsResource.OFFERS_LISTINGS_PRICE)
    items_resources.append(GetItemsResource.CUSTOMERREVIEWS_COUNT)
    items_resources.append(GetItemsResource.ITEMINFO_CONTENTRATING)
    items_resources.append(GetItemsResource.BROWSENODEINFO_BROWSENODES_SALESRANK)
    items_resources.append(GetItemsResource.BROWSENODEINFO_WEBSITESALESRANK)
    items_resources.append(GetItemsResource.IMAGES_PRIMARY_LARGE)
    items_resources.append(GetItemsResource.IMAGES_PRIMARY_MEDIUM)
    items_resources.append(GetItemsResource.IMAGES_PRIMARY_SMALL)
    items_resources.append(GetItemsResource.ITEMINFO_FEATURES)
    items_resources.append(GetItemsResource.ITEMINFO_CONTENTINFO)
    items_resources.append(GetItemsResource.ITEMINFO_BYLINEINFO)
    items_resources.append(GetItemsResource.ITEMINFO_PRODUCTINFO)

    return items_resources


def test(api_creds: AmazonPaApiCredentials):
    api_client = api_creds.get_api_client()

    search_items_request = SearchItemsRequest(
        partner_tag=api_creds.PartnerTag,
        partner_type=PartnerType.ASSOCIATES,
        item_page=3,
        item_count=40,
        browse_node_id="1981002031",
        sort_by=SortBy.PRICE_LOWTOHIGH,
        keywords=None,
        search_index="Fashion",
        resources=[],
    )

    # search_items_request = SearchItemsRequest(
    #     partner_tag=api_creds.PartnerTag,
    #     partner_type=PartnerType.ASSOCIATES,
    #     keywords="Oberkörper",
    #     search_index="All",
    #     resources=[],
    # )

    # get_items_request = GetItemsRequest(
    #     partner_tag=api_creds.PartnerTag,
    #     partner_type=PartnerType.ASSOCIATES,
    #     marketplace=f"www.amazon.de",
    #     condition=Condition.NEW,
    #     item_ids=["B000OM1XWI"],
    #     resources=get_items_resources(),
    # )
    return api_client.search_items(search_items_request)

if __name__ == '__main__':
    api_creds = AmazonPaApiCredentials("/home/fteutsch/MW/credentials/PAAPICredentials.csv", PartnerTag="merchwatch0f-21")
    t = test(api_creds)
    test = 0