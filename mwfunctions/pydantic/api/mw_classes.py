from pydantic import BaseModel, validator, Field
from typing import Optional, List, Union
from datetime import datetime, date, timedelta
from enum import IntEnum
from mwfunctions.pydantic.base_classes import Marketplace, MWBaseModel
from mwfunctions.pydantic.firestore.mba_shirt_classes import MBA_SHIRT_ORDERBY_DICT, MBAShirtOrderByField
from mwfunctions.pydantic.firestore.utils import get_bsr_range_list
from mwfunctions.cloud.firestore.commons import FSSimpleFilterQuery, FSComparisonOperator, OrderByDirection
from mwfunctions.text import get_stem_keywords_language, StemmerLanguage
from mwfunctions.constants.trademarks import TRADEMARKS

import json

class CloudFunctionWrapper(MWBaseModel):
    def get_dict(self):
        attr_dict  = {}
        for attr_key, attr_value in self._iter():
            attr_dict[attr_key] = attr_value
        return attr_dict

    def get_data(self):
        return json.dumps(self.get_dict()).encode('utf-8')


class ShirtDataPost(CloudFunctionWrapper):
    marketplace: Marketplace

    bsr_min: Optional[int] = 0
    bsr_max: Optional[int] = None
    price_min: Optional[int] = None
    price_max: Optional[int] = None
    upload_start: Optional[Union[date, datetime]] = None
    upload_end: Optional[Union[date, datetime]] = None
    admin: Optional[bool] = False
    cursor: Optional[str] = Field(None, description="Documen ID of FS e.g. ASIN")
    is_authenticated: Optional[bool] = False
    patreon_tier: Optional[int] = 0
    debug: Optional[bool] = False
    user_id: Optional[int] = 0
    language_code: Optional[str] = "de"

    @validator("upload_start", always=True)
    def validate_upload_start(cls, upload_start: Union[date, datetime]):
        # convert type(upload_start) == 'date' to datetime type for convinience
        upload_start: datetime = upload_start if type(upload_start) == datetime else datetime.combine(upload_start,
                                                                                                      datetime.min.time(),
                                                                                                      tzinfo=None) if upload_start else None
        return upload_start

    @validator("upload_end", always=True)
    def validate_upload_end(cls, upload_end: Union[date, datetime]):
        # convert type(upload_start) == 'date' to datetime type for convinience
        upload_end: datetime = upload_end if type(upload_end) == datetime else datetime.combine(upload_end,
                                                                                                datetime.max.time(),
                                                                                                tzinfo=None) if upload_end else None
        return upload_end

    def get_simple_query_filters(self) -> List[FSSimpleFilterQuery]:
        simple_query_filters = []
        if self.bsr_min:
            simple_query_filters.append(FSSimpleFilterQuery(field="bsr_last",
                                                            comparison_operator=FSComparisonOperator.GREATER_THAN,
                                                            value=self.bsr_min))
        if self.bsr_max:
            simple_query_filters.append(FSSimpleFilterQuery(field="bsr_last",
                                                            comparison_operator=FSComparisonOperator.LESS_THAN,
                                                            value=self.bsr_max))
        if self.price_min:
            simple_query_filters.append(FSSimpleFilterQuery(field="price_last",
                                                            comparison_operator=FSComparisonOperator.GREATER_THAN,
                                                            value=self.price_min))
        if self.price_max:
            simple_query_filters.append(FSSimpleFilterQuery(field="price_last",
                                                            comparison_operator=FSComparisonOperator.LESS_THAN,
                                                            value=self.price_max))
        if self.upload_start:
            simple_query_filters.append(FSSimpleFilterQuery(field="upload_date",
                                                            comparison_operator=FSComparisonOperator.GREATER_OR_EQUAL,
                                                            value=self.upload_start))
        if self.upload_end:
            simple_query_filters.append(FSSimpleFilterQuery(field="upload_date",
                                                            comparison_operator=FSComparisonOperator.LESS_OR_EQUAL,
                                                            value=self.upload_end))
        return simple_query_filters


class BatchDataPost(ShirtDataPost):
    sort_by: str
    page: Optional[int] = 1

    @validator("sort_by")
    def validate_sort_by(cls, sort_by, values):
        if sort_by[0] == "-":
            values["order_by"] = sort_by[1:len(sort_by)]
            values["order_by_direction"] = OrderByDirection.DESC
        else:
            values["order_by"] = sort_by
            values["order_by_direction"] = OrderByDirection.ASC
        return sort_by

    def get_simple_query_filters(self) -> List[FSSimpleFilterQuery]:
        simple_query_filters = super(BatchDataPost, self).get_simple_query_filters()

        def is_bsr_min_defined(bsr_min: Optional[int]) -> bool:
            return bool(bsr_min and bsr_min != MBA_SHIRT_ORDERBY_DICT[MBAShirtOrderByField.BSR.value].start_value)

        if is_bsr_min_defined(self.bsr_min) or self.bsr_max:
            bsr_range_start = int(self.bsr_min / 100000) if type(self.bsr_min) == int else 0
            bsr_range_end = int(self.bsr_max / 100000) if type(self.bsr_max) == int else 99
            simple_query_filters.append(FSSimpleFilterQuery(field="bsr_last_range",
                                                            comparison_operator=FSComparisonOperator.IN,
                                                            value=get_bsr_range_list((bsr_range_start, bsr_range_end),
                                                                                     min(bsr_range_end - bsr_range_start + 1,
                                                                                         51))
                                                            ))

        return simple_query_filters


class NichePlotPost(CloudFunctionWrapper):
    niche_id: str
    marketplace: str

    date_last: Optional[str] = str(datetime.now().date())
    language_code: Optional[str] = "de"


class NicheDataPost(BatchDataPost):
    niche_type: str
    niche_count: int


class UploadSinceDays(IntEnum):
    ONE_WEEK = 7
    TWO_WEEKS = 14
    ONE_MONTH = 30
    THREE_MONTHS = 90
    ONE_YEAR = 365

    def get_upload_start(self) -> date:
        return datetime.now() - timedelta(days=self)


class SearchPost(BaseModel):
    marketplace: Marketplace

    stem_keywords: Optional[List[str]] = Field(None, description="Stemmed keywords list is set by key field")
    key: Optional[str] = None

    @validator("key")
    def validate_search_key(cls, key, values):
        # make sure key is None if no search key is provided
        if key == "" or key == None: return None
        # https://firebase.google.com/docs/firestore/query-data/queries#array-contains-any -> max 10 values in list are allowed
        if values["marketplace"] == Marketplace.DE:
            values["stem_keywords"]: list = get_stem_keywords_language(key.lower().split(" "), StemmerLanguage.GERMAN)[
                                            0:10]
        else:
            values["stem_keywords"]: list = get_stem_keywords_language(key.lower().split(" "), StemmerLanguage.ENGLISH)[
                                            0:10]
        return key

    def get_simple_query_filters(self) -> List[FSSimpleFilterQuery]:
        # TODO: filter keywords in fs with array contains any but afterwards with array contains all

        simple_query_filters = []
        if self.key:
            # simple_query_filters.append(FSSimpleFilterQuery(field="keywords_stem_list",
            #                                                 comparison_operator=FSComparisonOperator.ARRAY_CONTAINS_ALL,
            #                                                 value=self.stem_keywords))
            for stem_keyword in self.stem_keywords:
                simple_query_filters.append(FSSimpleFilterQuery(field=f"keywords_stem.`{stem_keyword}`",
                                                                comparison_operator=FSComparisonOperator.EQUAL,
                                                                value=True))

        return simple_query_filters


class WatchDataPost(BatchDataPost, SearchPost):
    shirt_count: int

    only_affiliate: Optional[bool] = False
    sortby_cursor: Optional[Union[float, int, date, datetime, str]] = None
    watchlist_asins: Optional[list] = []
    filter_trademarks: Optional[bool] = False
    filter_takedowns: bool = True
    get_watchlist: Optional[bool] = False
    trademark_count: Optional[int] = 0
    upload_since_days: Optional[UploadSinceDays] = None

    @validator("sortby_cursor", always=True)
    def validate_sortby_cursor(cls, sortby_cursor, values):
        # TODO use some kind of class to define start values for different order by statementd
        if sortby_cursor:
            return sortby_cursor
        else:
            if values["order_by"] in MBA_SHIRT_ORDERBY_DICT and values["order_by_direction"] == MBA_SHIRT_ORDERBY_DICT[
                values["order_by"]].direction:
                return MBA_SHIRT_ORDERBY_DICT[values["order_by"]].start_value
            else:
                return None

    def get_simple_query_filters(self) -> List[FSSimpleFilterQuery]:
        simple_query_filters = []
        for cls in WatchDataPost.__bases__:
            simple_query_filters = simple_query_filters + cls.get_simple_query_filters(self)

        if self.sortby_cursor:
            comparision_operator = FSComparisonOperator.GREATER_OR_EQUAL if self.order_by_direction == OrderByDirection.ASC else FSComparisonOperator.LESS_OR_EQUAL
            sortby_cursor = self.sortby_cursor if type(self.sortby_cursor) != date else datetime.combine(
                self.sortby_cursor, datetime.min.time())
            simple_query_filters.append(FSSimpleFilterQuery(field=self.order_by,
                                                            comparison_operator=comparision_operator,
                                                            value=sortby_cursor))
        # if no sortby cursor exists and its trend or bsr order by, set a default filter value e.g. to prevent showing bsr_last 0 shirts
        elif self.sort_by in [MBAShirtOrderByField.TREND, MBAShirtOrderByField.BSR]:
            simple_query_filters.append(FSSimpleFilterQuery(field=self.sort_by,
                                                            comparison_operator=FSComparisonOperator.GREATER_THAN,
                                                            value=0.0001))
        if self.filter_trademarks:
            # TODO: Add is_trademarked to MBA Shirt FS document
            # simple_query_filters.append(FSSimpleFilterQuery(field="is_trademarked",
            #                             comparison_operator=FSComparisonOperator.EQUAL, value=False))
            simple_query_filters.append(FSSimpleFilterQuery(field="brand",
                                                            comparison_operator=FSComparisonOperator.TEXT_CONTAINS_NOT_ANY,
                                                            value=TRADEMARKS))
            simple_query_filters.append(FSSimpleFilterQuery(field="keywords_meaningful",
                                                            comparison_operator=FSComparisonOperator.ARRAY_CONTAINS_NOT_ANY,
                                                            value=TRADEMARKS))
            pass
        if self.upload_since_days:
            simple_query_filters.append(FSSimpleFilterQuery(field="upload_date",
                                                            comparison_operator=FSComparisonOperator.GREATER_THAN,
                                                            value=self.upload_since_days.get_upload_start()))
        if self.filter_takedowns:
            simple_query_filters.append(FSSimpleFilterQuery(field="takedown",
                                                            comparison_operator=FSComparisonOperator.EQUAL,
                                                            value=False))

        return simple_query_filters


class WatchMetaDataPost(BatchDataPost, SearchPost):
    sort_by: str = MBAShirtOrderByField.BSR
    filter_trademarks: Optional[bool] = False
    type: Optional[str] = "search_niche"
    shirt_count: Optional[int] = 50
    upload_since_days: Optional[UploadSinceDays] = None

    def get_simple_query_filters(self, for_takedowns=False) -> List[FSSimpleFilterQuery]:
        # TODO try to add filter for self.upload_since_days
        simple_query_filters = []
        for cls in WatchDataPost.__bases__:
            simple_query_filters = simple_query_filters + cls.get_simple_query_filters(self)

        if self.filter_trademarks:
            # TODO: Add is_trademarked to MBA Shirt FS document
            simple_query_filters.append(FSSimpleFilterQuery(field="is_trademarked",
                                                            comparison_operator=FSComparisonOperator.EQUAL,
                                                            value=False))
        if for_takedowns:
            simple_query_filters.append(FSSimpleFilterQuery(field="takedown",
                                                            comparison_operator=FSComparisonOperator.EQUAL,
                                                            value=True))
        else:
            simple_query_filters.append(FSSimpleFilterQuery(field="takedown",
                                                            comparison_operator=FSComparisonOperator.EQUAL,
                                                            value=False))

        return simple_query_filters


class WatchPlotPost(CloudFunctionWrapper):
    watch_id: str  # Is asin in general
    marketplace: str

    date_last: Optional[str] = str(datetime.now().date())
    language_code: Optional[str] = "de"

