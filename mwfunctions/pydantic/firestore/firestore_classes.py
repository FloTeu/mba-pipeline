from __future__ import annotations

import copy
from enum import Enum
from typing import Union, Dict, List, Optional, Any
from pydantic import BaseModel, Field, PrivateAttr, validator
from datetime import date, datetime
from google.cloud.firestore import DocumentSnapshot
from mwfunctions.pydantic.base_classes import MWBaseModel, Marketplace, EnumBase

def date2str(dict_obj):
    # transform date values to strings, because FS cant store date format (only datetime or string)
    for key, value in dict_obj.items():
        if isinstance(value, date):
            dict_obj[key] = str(value)

class FSDocument(MWBaseModel):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    _fs_col_path: str = PrivateAttr() # Full collection path under which doc_id can be foundin Firestore
    _fs_subcollections: Dict[str, FSSubcollection] = PrivateAttr({}) # str is subcollection_col_name of FSSubcollection
    _overwrite_doc: bool = PrivateAttr(False)
    _array_union: bool = PrivateAttr(False)
    doc_id: str = Field(description="Firestore document id")

    @classmethod
    def parse_fs_doc_snapshot(cls, doc_snap: DocumentSnapshot) -> FSDocument:
        fs_col_path = "/".join(doc_snap.reference.path.split("/")[0:-1])
        return cls.parse_obj({"doc_id": doc_snap.id, **doc_snap.to_dict()}).set_fs_col_path(fs_col_path)

    def set_fs_col_path(self, fs_col_path) -> FSDocument:
        self._fs_col_path = fs_col_path
        return self

    def get_fs_col_path(self):
        return self._fs_col_path

    def get_fs_doc_path(self):
        return f"{self._fs_col_path}/{self.doc_id}"

    def update_fs_subcollections(self, subcollection: FSSubcollection):
        # update subcollection of fs_document and sets all _fs_col_path of all documents within subcollection
        for doc_id, subcollection_doc in subcollection.subcollection_doc_dict.items():
            subcollection_doc.set_fs_col_path(f"{self._fs_col_path}/{self.doc_id}/{subcollection.subcollection_col_name}")
        self._fs_subcollections[subcollection.subcollection_col_name] = subcollection

    def write_to_firestore(self, exclude_doc_id=False, exclude_fields=[], write_subcollections=True, client=None):
        # load module in function to prevent circular import
        from mwfunctions.cloud.firestore import split_correct_path, write_document_dict

        exclude_fields = exclude_fields + ["doc_id"] if exclude_doc_id else exclude_fields
        dict_to_fs = self.dict(exclude=set(exclude_fields))
        date2str(dict_to_fs)
        write_document_dict(dict_to_fs, f"{self._fs_col_path}/{self.doc_id}", array_union=self._array_union, overwrite_doc=self._overwrite_doc, client=client)
        if write_subcollections:
            for subcollection_col_name, fs_subcollection in self._fs_subcollections.items():
                for doc_id, fs_document in fs_subcollection.subcollection_doc_dict.items():
                    fs_document.write_to_firestore(exclude_doc_id=exclude_doc_id, exclude_fields=exclude_fields, write_subcollections=write_subcollections, client=client)

    #
    # def update_fs_subcollections(self, subcollection_col_name, subcollection_doc: FSDocument):
    #     subcollection_doc.set_fs_col_path(f"{self._fs_col_path}/{self.doc_id}/{subcollection_col_name}")
    #     if subcollection_col_name not in self._fs_subcollections:
    #         self._fs_subcollections[subcollection_col_name] = [subcollection_doc]
    #     else:
    #         self._fs_subcollections[subcollection_col_name].append(subcollection_doc)

class FSSubcollection(MWBaseModel):
    subcollection_col_name: str = Field(description="col name of sub collection e.g. plot_data")
    subcollection_doc_dict: Optional[Dict[str,FSDocument]] = Field({}, description="All documents contained in subcollection. key of dict is doc_id of FSDocument")

    def update_subcollection_doc_dict(self, subcollection_doc: FSDocument):
        self.subcollection_doc_dict[subcollection_doc.doc_id] = subcollection_doc

# class FSWatchItemPlotDataNumber(MWBaseModel):
#     plot_y:

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
    # TODO: Use child of FSSubcollection in future
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
    subcollection_col_name: str = "plot_data"
    subcollection_doc_dict: Optional[Dict[str, FSWatchItemSubCollectionPlotDataYear]] = Field({})# str/key is year


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
        if self.bsr_short == None and self.plot_x != None:
            plot_x = copy.deepcopy(self.plot_x)
            plot_x.reverse()
            plot_y = copy.deepcopy(self.plot_y)
            plot_y.reverse()
            # TODO: try to change format of key to date not date_str
            self.bsr_short = {str(datetime.strptime(date_str, "%d/%m/%Y").date()): bsr for date_str, bsr in zip(plot_x,plot_y)}

    def set_prices_short(self):
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

class FSMBAShirt(FSDocument, FSWatchItemShortenedPlotData):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    _fs_subcollections: Dict[str, Union[FSWatchItemSubCollectionPlotData]] = PrivateAttr({})
    # marketplace: Optional[Marketplace] = Field(None, description="Currently not existent in FS, but if known it can be provided to directly set fs_col_path")
    # TODO: add oher fields of fireste document
    asin: str

    bsr_last: int
    bsr_first: int
    bsr_max: int
    bsr_min: int
    bsr_mean: int
    bsr_category: str
    bsr_last_change: int
    bsr_change_old: Optional[Union[int, float]] = None
    bsr_change: Union[int, float]
    bsr_change_total: Optional[Union[int, float]] = None
    bsr_last_range: int
    bsr_count: int

    title: str
    brand: str

    price_last: float
    price_max: float
    price_min: float
    price_first: float
    price_change: float
    price_mean: float

    price_last_ranges_array: List[int]
    price_last_range: int

    # TODO: make image urls only required if takedown == False
    url_mba_lowq: str
    url_image_q2: str
    url_image_q3: str
    url_image_q4: str
    url_mba_hq: str
    url: Optional[str] = Field(None, description="Http url to private stored image")

    keywords_stem_list: Optional[List[str]] = None
    keywords_meaningful: List[str]
    keywords_stem: Dict[str, bool]

    upload_date: Union[datetime, date]
    upload_since_days: Optional[int] = None
    upload_since_days_map: Optional[Dict[str, bool]] = None
    time_since_upload: Optional[float] = None

    trend_nr: int
    trend: float
    trend_change: int

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


    @validator("keywords_stem")
    def set_keywords(cls, keywords_stem, values):
        if "keywords_stem_list" not in values or values["keywords_stem_list"] == None:
            values["keywords_stem_list"] = list(keywords_stem.keys())
        return keywords_stem


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

class OrderByDirection(str, Enum):
    ASC="asc"
    DESC="desc"

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

