from pydantic import BaseModel, Field, PrivateAttr, validator
from mwfunctions.pydantic.base_classes import MWBaseModel, Marketplace, EnumBase

from typing import List, Optional


class FSIndexOption(str, EnumBase):
    ASC="asc"
    DESC="desc"
    ARRAY="array"

class FSIndexField(str, EnumBase):
    pass

class FSIndexQueryScope(str, EnumBase):
    COLLECTION="collection"
    COLLECTION_GROUP="collection_group"

class FSIndexItem(BaseModel):
    field: FSIndexField
    index_option: FSIndexField
    query_scope: FSIndexQueryScope = Field(FSIndexQueryScope.COLLECTION)
    collection: Optional[str] = Field(description="Name of collection. Can be de_shirts as an example")

class FSIndex(BaseModel):
    index_items: List[FSIndexItem]

class FSMbaShirtsIndexField(FSIndexField):
    # index fields are all order by fields plus special filter fields
    BSR="bsr_last"
    PRICE="price_last"
    TREND="trend_nr"
    UPLOAD="upload_date"
    BSR_CHANGE="bsr_change"
    TAKEDOWN="takedown"
    BSR_RANGE="bsr_last_range"
    KEYWORD_LIST="keywords_stem_list"

class FSMbaShirtsIndexItem(FSIndexItem):
    field: FSMbaShirtsIndexField
    index_option: FSIndexOption

class FSMbaShirtsIndex(FSIndex):
    index_items: List[FSMbaShirtsIndexItem]

# defines all collection indexes that exist in FS for collection {marketplace}_shirts
MBA_SHIRTS_COLLECTION_INDEXES: List[FSMbaShirtsIndex] = [
    # keywords_stem_list: ARRAY, takedown: ASC, bsr_last: ASC
    FSMbaShirtsIndex.parse_obj(
        {"index_items": [{"field": FSMbaShirtsIndexField.KEYWORD_LIST, "index_option": FSIndexOption.ARRAY},
                                                {"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                                                {"field": FSMbaShirtsIndexField.BSR, "index_option": FSIndexOption.ASC}]}),
    # bsr_last_range: ASC, takedown: ASC, trend_nr: ASC
    FSMbaShirtsIndex.parse_obj(
        {"index_items": [{"field": FSMbaShirtsIndexField.BSR_RANGE, "index_option": FSIndexOption.ASC},
                         {"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                         {"field": FSMbaShirtsIndexField.TREND, "index_option": FSIndexOption.ASC}]}),
    # bsr_last_range: ASC, takedown: ASC, upload_date: DESC
    FSMbaShirtsIndex.parse_obj(
            {"index_items": [{"field": FSMbaShirtsIndexField.BSR_RANGE, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.UPLOAD, "index_option": FSIndexOption.DESC}]}),
    # bsr_last_range: ASC, takedown: ASC, bsr_change: ASC
    FSMbaShirtsIndex.parse_obj(
            {"index_items": [{"field": FSMbaShirtsIndexField.BSR_RANGE, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.BSR_CHANGE, "index_option": FSIndexOption.ASC}]}),
    # bsr_last_range: ASC, takedown: ASC, price_last: ASC
    FSMbaShirtsIndex.parse_obj(
            {"index_items": [{"field": FSMbaShirtsIndexField.BSR_RANGE, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.PRICE, "index_option": FSIndexOption.ASC}]}),
    # takedown: ASC, trend_nr: ASC
    FSMbaShirtsIndex.parse_obj(
        {"index_items": [{"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                         {"field": FSMbaShirtsIndexField.TREND, "index_option": FSIndexOption.ASC}]}),
    # takedown: ASC, upload_date: DESC
    FSMbaShirtsIndex.parse_obj(
            {"index_items": [{"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.UPLOAD, "index_option": FSIndexOption.DESC}]}),
    # takedown: ASC, bsr_change: ASC
    FSMbaShirtsIndex.parse_obj(
            {"index_items": [{"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.BSR_CHANGE, "index_option": FSIndexOption.ASC}]}),
    # takedown: ASC, bsr_last: ASC
    FSMbaShirtsIndex.parse_obj(
            {"index_items": [{"field": FSMbaShirtsIndexField.TAKEDOWN, "index_option": FSIndexOption.ASC},
                             {"field": FSMbaShirtsIndexField.BSR, "index_option": FSIndexOption.ASC}]})
]