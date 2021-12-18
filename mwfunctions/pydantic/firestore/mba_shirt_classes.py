import copy
from datetime import datetime, date
from typing import Dict, Union, Optional, List, Any

from mwfunctions.pydantic import FSDocument, MWBaseModel, FSSubcollection, EnumBase
from mwfunctions.cloud.firestore import OrderByDirection
from pydantic import Field, validator, PrivateAttr, BaseModel


class FSWatchItemSubCollectionPlotDataYear(FSDocument):
    bsr: Dict[str, Union[int, float]] # str is date_str
    prices: Dict[str, Union[int, float]] # str is date_str
    year: int
    doc_id: Optional[str] = Field(description="Firestore document id")

    @validator("year")
    def set_doc_id(cls, year, values):
        values["doc_id"] = str(year)
        return year


class FSWatchItemSubCollectionDict(MWBaseModel):
    """ Example:
            {
                "plot_data":
                    {
                        "year":
                            {"bsr": {"2020-09-20": 480549, ...},
                            "price": {"2020-09-20": 13.99, ...}
                            }
                    }
            }
    """
    # TODO: Use child of FSSubcollection in future (currently this class is needed for firestore update function in daily cron job)
    plot_data: Dict[str, FSWatchItemSubCollectionPlotDataYear] # str/key is year


class FSWatchItemSubCollectionPlotData(FSSubcollection):
    """ Example:
            {
                "plot_data":
                    {
                        "year":
                            {"bsr": {"2020-09-20": 480549, ...},
                            "price": {"2020-09-20": 13.99, ...}
                            }
                    }
            }
    """
    col_name: str = "plot_data"
    doc_dict: Optional[Dict[str, FSWatchItemSubCollectionPlotDataYear]] = Field({})# str/key is year

    @classmethod
    def get_subcollection_col_name(cls):
        return "plot_data"

class FSWatchItemShortenedPlotData(MWBaseModel):
    """ Example:
            {
                "bsr_short":
                    {"2020-09-20": 480549, ...}
                "prices_short":
                    {"2020-09-20": 480549, ...}
            }
    """

    # TODO: try to change format of key to date not date_str
    bsr_short: Optional[Dict[str, int]] = Field(None, description="Dict with date as key and bsr as value. First value is most in past last closest to present") # str is date_str
    prices_short: Optional[Dict[str, float]] = Field(None, description="Dict with date as key and price in float as value. First value is most in past last closest to present") # str is date_str

    # init deprecated values later, because they can than be initilaised by bsr_short and prices_short
    plot_x: Optional[Union[str, List[str]]] = Field(None, description="Deprecated. List of x axis data, which are dates in '%d/%m/%Y' format")
    plot_y: Optional[Union[str, List[int]]] = Field(None, description="Deprecated. List of y axis data, which is a list of bsr")
    plot_x_price: Optional[Union[str, List[str]]] = Field(None, description="Deprecated. List of x axis data, which are dates in '%d/%m/%Y' format")
    plot_y_price: Optional[Union[str, List[float]]] = Field(None, description="Deprecated. List of y axis data, which is a list of prices stringifed from floats")

    def set_bsr_short(self):
        # case old structure with plot_x etc. stored in FS but not bsr_short
        if self.bsr_short == None and self.plot_x != None:
            plot_x = copy.deepcopy(self.plot_x)
            plot_x.reverse()
            plot_y = copy.deepcopy(self.plot_y)
            plot_y.reverse()
            # TODO: try to change format of key to date not date_str
            self.bsr_short = {str(datetime.strptime(date_str, "%d/%m/%Y").date()): bsr for date_str, bsr in zip(plot_x,plot_y)}

    def set_prices_short(self):
        # case old structure with plot_x_price etc. stored in FS but not price_short
        if self.prices_short == None and self.plot_x_price != None:
            plot_x_price = copy.deepcopy(self.plot_x_price)
            plot_x_price.reverse()
            plot_y_price = copy.deepcopy(self.plot_y_price)
            plot_y_price.reverse()
            # TODO: try to change format of key to date not date_str
            self.prices_short = {str(datetime.strptime(date_str, "%d/%m/%Y").date()): price for date_str, price in zip(plot_x_price,plot_y_price)}

    # followwing 4 validators:
    # Backwards comp for MW API. Frontend requires a string with comma sep for getting plot data
    # TODO: Only shortened data (new) is created by old data not the other ways around. Not possible with validator unfortunatly. Check if frontend still works with new format
    # TODO: Seems like frontend requires old structure with plot_x, plot_y ...
    # TODO: If new data is added, we dont want plot_x to be set..

    @validator("plot_x", always=True)
    def set_plot_x(cls, plot_x, values):
        bsr_short_plot_x = [datetime.strptime(str(date_str), '%Y-%m-%d').strftime('%d/%m/%Y') for date_str in list(values["bsr_short"].keys())] if values["bsr_short"] else None
        if bsr_short_plot_x: bsr_short_plot_x.reverse()
        return plot_x or bsr_short_plot_x

    @validator("plot_y", always=True)
    def set_plot_y(cls, plot_y, values):
        bsr_short_plot_y = list(values["bsr_short"].values()) if values["bsr_short"] else None
        if bsr_short_plot_y: bsr_short_plot_y.reverse()
        return plot_y or bsr_short_plot_y

    @validator("plot_x_price", always=True)
    def set_plot_x_price(cls, plot_x_price, values):
        prices_short_plot_x = [datetime.strptime(str(date_str), '%Y-%m-%d').strftime('%d/%m/%Y') for date_str in list(values["prices_short"].keys())] if values["prices_short"] else None
        if prices_short_plot_x: prices_short_plot_x.reverse()
        return plot_x_price or prices_short_plot_x

    @validator("plot_y_price", always=True)
    def set_plot_y_price(cls, plot_y_price, values):
        prices_short_plot_y = list(values["prices_short"].values()) if values["prices_short"] else None
        if prices_short_plot_y: prices_short_plot_y.reverse()
        return plot_y_price or prices_short_plot_y

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.set_bsr_short()
        self.set_prices_short()

class FSBSRData(MWBaseModel):
    bsr_last: int
    bsr_first: int
    bsr_max: int
    bsr_min: int
    bsr_mean: int
    bsr_category: str
    bsr_last_change: int
    bsr_change: Union[int, float] = Field(description="Bsr change of last month. Is used for merchwatch watch page sorting by strongest change.")
    bsr_change_total: Optional[Union[int, float]] = None
    bsr_last_range: int = Field(description="Small Integer representation of bsr_last. 0 means bsr_last is between 0-100.000. Every increments is a 100000 range. 99 if higher than 5.000.000")
    bsr_count: int

    @validator("bsr_last_range", always=True)
    def set_bsr_last_range(cls, bsr_last_range, values):
        # 99 stands for higher than max filterable value, i.e. 50.000.000
        return bsr_last_range if bsr_last_range else int(values["bsr_last"] / 100000) if values["bsr_last"] < 5000000 else 99


class FSPriceData(MWBaseModel):
    price_last: float
    price_max: float
    price_min: float
    price_first: float
    price_change: float
    price_mean: float

    price_last_ranges_array: List[int]
    price_last_range: int

class FSImageData(MWBaseModel):
    # TODO: make image urls only required if takedown == False
    url_mba_lowq: str
    url_image_q2: str
    url_image_q3: str
    url_image_q4: str
    url_mba_hq: str
    url: Optional[str] = Field(None, description="Http url to private stored image")

class FSKeywordData(MWBaseModel):
    keywords_stem: Dict[str, bool]

    # TODO: replace keywords_meaningful with splitted keyword list. Can be concatinated dynamically afterwards
    keywords_brand: Optional[List[str]] = Field(None, description="Optional due to backwards comp. Should be required in future")
    keywords_title: Optional[List[str]] = Field(None, description="Optional due to backwards comp. Should be required in future")
    keywords_listings: Optional[List[str]] = Field(None, description="Optional due to backwards comp. Should be required in future")
    keywords_description: Optional[List[str]] = Field(None, description="Optional due to backwards comp. Should be required in future")

    keywords_meaningful: Optional[List[str]] = Field(None, description="List of meaningful keywords. Can be full word (not stemmed) or multiple words (long tail keywords). If not set it will be created by brand, title, listings and description")

    # field keywords_stem_list is not needed, because keywords search does not need index on this list
    # keywords_stem_list: Optional[List[str]] = None
    # @validator("keywords_stem")
    # def set_keywords(cls, keywords_stem, values):
    #     if "keywords_stem_list" not in values or values["keywords_stem_list"] == None:
    #         values["keywords_stem_list"] = list(keywords_stem.keys())
    #     return keywords_stem

    @validator("keywords_meaningful", always=True)
    def set_keywords_meaningful(cls, keywords_meaningful, values):
        keyword_lists_field_strs = ["keywords_brand", "keywords_title", "keywords_listings", "keywords_description"]
        if not keywords_meaningful:
            keywords_meaningful = []
            for keyword_lists_field_str in keyword_lists_field_strs:
                keywords_meaningful.extend(keyword_lists_field_str)
        return list(set(keywords_meaningful))

class FSUploadData(MWBaseModel):
    upload_date: Union[datetime, date]
    upload_since_days: Optional[int] = None
    upload_since_days_map: Optional[Dict[str, bool]] = None
    time_since_upload: Optional[float] = None

class FSTrendData(MWBaseModel):
    trend_nr: int
    trend: float
    trend_change: int

class FSMBAShirt(FSDocument, FSWatchItemShortenedPlotData, FSBSRData, FSPriceData, FSImageData, FSKeywordData, FSUploadData, FSTrendData):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    _fs_subcollections: Dict[str, Union[FSWatchItemSubCollectionPlotData]] = PrivateAttr({})
    # marketplace: Optional[Marketplace] = Field(None, description="Currently not existent in FS, but if known it can be provided to directly set fs_col_path")
    asin: str

    title: str
    brand: str

    is_trademarked: bool = Field(False, description="Whether product is trademarked. If True they can be filtered")

    takedown: bool
    takedown_date: Optional[date] = None

    score_count: Optional[int] = None
    score_last: Optional[float] = None
    score_last_rounded: Optional[int] = None

    language: Optional[str] = None
    timestamp: datetime
    time_since_upload_power: Optional[float] = None
    update_last: date
    url_affiliate: Optional[str] = None
    img_affiliate: Optional[str] = None
    affiliate_exists: Optional[bool] = None


    def set_bsr_change(self):
        """ If subcollections exists take all bsr data to update bsr_change, otherwise take shortened bsr_short plot data
        """
        plot_data_col_name = FSWatchItemSubCollectionPlotData.get_subcollection_col_name()
        if plot_data_col_name in self._fs_subcollections:
            for year, plot_data in self._fs_subcollections[plot_data_col_name].doc_dict.items():
                pass


    def get_api_dict(self, meta_api=False):
        # transform to required backwards compatabile format
        plot_x = [datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d') for date_str in self.plot_x] if self.plot_x else None
        plot_x_price = [datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d') for date_str in self.plot_x_price] if self.plot_x_price else None
        plot_y = [str(bsr) for bsr in self.plot_y] if self.plot_y else None
        plot_y_price = [str(bsr) for bsr in self.plot_y_price] if self.plot_y_price else None

        plot_data = {"plot_x": ",".join(plot_x) if plot_x else None,
                     "plot_y": ",".join(plot_y) if plot_y else None,
                     "plot_x_price": ",".join(plot_x_price) if plot_x_price else None,
                     "plot_y_price": ",".join(plot_y_price) if plot_y_price else None}

        fields_included = {"asin", "bsr_short", "prices_short", "bsr_change", "bsr_mean", "bsr_last", "keywords_meaningful", "url", "url_affiliate", "url_mba_hq", "url_mba_lowq", "url_image_q2", "url_image_q3", "url_image_q4", "price_last", "update_last", "img_affiliate", "title", "brand", "trend_nr", "trend_change", "upload_date", "takedown", "takedown_date"}
        api_output_dict = self.dict(include=fields_included)
        if not meta_api:
            api_output_dict["upload_date"] = api_output_dict["upload_date"].strftime(format="%Y-%m-%dT%H:%M:%SZ") if isinstance(api_output_dict["upload_date"], datetime) else api_output_dict["upload_date"]
        else:
            # meta api requires list of plot data
            plot_data = {"plot_x": plot_x if plot_x else None,
                         "plot_y": plot_y if plot_y else None,
                         "plot_x_price": plot_x_price if plot_x_price else None,
                         "plot_y_price": plot_y_price if plot_y_price else None}
            api_output_dict["takedown_date"] = str(api_output_dict["takedown_date"])

        api_output_dict.update(plot_data)
        return api_output_dict

    # def __init__(self, **data: Any) -> None:
    #     super().__init__(**data)
    #     if self.marketplace:
    #         self.set_fs_col_path( f"{self.marketplace}_shirts")


class MBAShirtOrderByField(str, EnumBase):
    # value is FS field
    BSR="bsr_last"
    PRICE="price_last"
    TREND="trend_nr"
    UPLOAD="upload_date"
    BSR_CHANGE="bsr_change"


class FSMBAShirtOrderBy(BaseModel):
    fs_field: MBAShirtOrderByField
    direction: OrderByDirection
    start_value: Optional[Union[float, int, datetime, str]] = Field(description="Start value for getting first element in FS. Depends on direction")

# key is string to make operation like string in MBA_SHIRT_ORDERBY_DICT possible
MBA_SHIRT_ORDERBY_DICT: Dict[MBAShirtOrderByField, FSMBAShirtOrderBy] = {
    MBAShirtOrderByField.BSR.value: FSMBAShirtOrderBy(fs_field=MBAShirtOrderByField.BSR, direction=OrderByDirection.ASC, start_value=0),
    MBAShirtOrderByField.PRICE.value: FSMBAShirtOrderBy(fs_field=MBAShirtOrderByField.PRICE, direction=OrderByDirection.ASC, start_value=10.0),
    MBAShirtOrderByField.TREND.value: FSMBAShirtOrderBy(fs_field=MBAShirtOrderByField.TREND, direction=OrderByDirection.ASC, start_value=0),
    MBAShirtOrderByField.UPLOAD.value: FSMBAShirtOrderBy(fs_field=MBAShirtOrderByField.UPLOAD, direction=OrderByDirection.DESC, start_value=datetime.max),
    MBAShirtOrderByField.BSR_CHANGE.value: FSMBAShirtOrderBy(fs_field=MBAShirtOrderByField.BSR_CHANGE, direction=OrderByDirection.ASC, start_value=-100000000),
}


def bsr2bsr_range_value(bsr) -> int:
    # returns value between 0 and 50
    # old code: return (int(self.bsr_min/100000),math.ceil(self.bsr_max/100000))
    return int(bsr/100000)


def get_bsr_range_list(bsr_last_range: tuple, bsr_range_to_query):
    try:
        bsr_range_list = []
        if type(bsr_last_range) == tuple:
            bsr_start = bsr_last_range[0] if type(bsr_last_range[0]) == int else 0
            bsr_end = bsr_last_range[1] if type(bsr_last_range[1]) == int and bsr_last_range[1] < (bsr_start + bsr_range_to_query) else bsr_start + bsr_range_to_query # firestore filter does allow only 10 values for "IN" query
            if bsr_end > 51:
                bsr_end = 52
            # catch the case get designs without bsr
            if bsr_start > (50-bsr_range_to_query) and bsr_last_range[1] == None:
                bsr_end = bsr_end - 1
                bsr_range_list = [i for i in range(bsr_start, bsr_end)] + [99]
            else:
                bsr_range_list = [i for i in range(bsr_start, bsr_end)]
            return bsr_range_list
        else:
            return bsr_range_list
    except Exception as e:
        print(str(e))
        return []




# deprecated

    # master_filter: Optional[int] = Field(None, description="Master filter contains multiple other filter values like bsr price, review and upload_date.")
    # @staticmethod
    # def get_master_filter(bsr_last: int, price_last: float, upload_date: Union[datetime, date], review_score: float) -> int:
    #     """ Creates the master filter value
    #         Structure:
    #             0000    00  00  00    0      000000000
    #             Y       M   D   price   review  bsr
    #
    #         TODO: Problems with master_filter:
    #             * order by trend_nr plus master_filter not possible
    #             * hugh amount of or conditions between small < > filters (will produce other problems like loading time, parallel query requirements)
    #
    #         Examples:
    #             2021031023997034545123
    #             -> upload_date 2021, 03, 10
    #             -> price 23,99 -> 24 | 23,49 -> 23
    #             -> review 3,9 -> {0-0.5:0, 0.5-1:1, 1-1.5:2, ..., 4-4.5:8, 4.5-5:9 }
    #             -> bsr 34545123
    #
    #         FS Filtering:
    #             * Example: Try to get data with requirements:
    #                 * upload_date between: (2020,2,1)-(2021,7,1)
    #                 * price between: 10-14
    #                 * bsr between: 150000-250000
    #             * Filter integers:
    #                 * > 20200201 10 0 150000 < 20200201 1000 0 250000
    #                 * > 20200201 10 1 150000 < 20200201 1000 1 250000
    #                 * > 20200201 10 2 150000 < 20200201 1000 2 250000
    #                                         ...
    #                 * > 20200201 11 0 150000 < 20200201 1000 2 250000
    #                 * > 20200201 11 1 150000 < 20200201 1000 1 250000
    #                                         ...
    #                 * > 20200202 10 0 150000 < 20200202 1000 0 250000
    #                 * > 20200202 10 1 150000 < 20200202 1000 1 250000
    #
    #             * Filter execution:
    #                 * FSSimpleFilterQuery(field="master_filter", comparison_operator=FSComparisonOperator.GREATER_OR_EQUAL, value=20200201 10 0 150000)
    #                 * FSSimpleFilterQuery(field="master_filter", comparison_operator=FSComparisonOperator.LESS_OR_EQUAL, value=20200201 10 0 250000)
    #                 * FSSimpleFilterQuery(field="master_filter", comparison_operator=FSComparisonOperator.GREATER_OR_EQUAL, value=20200201 10 1 150000)
    #                 * FSSimpleFilterQuery(field="master_filter", comparison_operator=FSComparisonOperator.LESS_OR_EQUAL, value=20200201 10 1 150000 )
    #     """
    #     bsr_str=format(bsr_last, "09d")
    #     assert len(bsr_str)==9, "BSR string must have length 9"
    #     price_str=format(int("%.0f" % price_last), "02d")
    #     assert len(price_str)==2, "Price string must have length 4"
    #     review_score_str=str(int("%.0f" % (review_score*2))-1) if review_score != 0 else "0"
    #     assert len(review_score_str)==1, "review_score_str string must have length 1"
    #     upload_int=date_to_integer(upload_date)
    #     return int(f"{upload_int}{price_str}{bsr_str}")


    # @validator("master_filter", always=True)
    # def set_master_filter(cls, master_filter, values):
    #     if master_filter: return master_filter
    #     return cls.get_master_filter(values["bsr_last"], values["price_last"], values["upload_date"], values["review_score"] if "review_score" in values else 0)
    #
