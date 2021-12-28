import copy
import collections
import re
from datetime import datetime, date, timedelta
from typing import Dict, Union, Optional, List, Any, Type

from mwfunctions.transform.plot_data_fns import get_shortened_plot_data
from mwfunctions.pydantic import FSDocument, MWBaseModel, FSSubcollection, EnumBase, Marketplace, GetFSDocsSettings
from mwfunctions.pydantic.firestore.trend_utils import get_trend_multiplicator
from mwfunctions.cloud.firestore import OrderByDirection, get_document_snapshot, get_docs_batch
from pydantic import Field, validator, PrivateAttr, BaseModel


class FSWatchItemSubCollectionPlotDataYear(FSDocument):
    bsr: Dict[str, int] # str is date_str
    bsr_category: Dict[str, str] = Field({}, description="Optional dict sync to bsr which defines bsr_category as value e.g. Bekleidung")
    prices: Dict[str, float] # str is date_str
    scores: Dict[str, float] = Field({}, description="Dict with date_str as key and mean review score as value")
    year: int
    doc_id: Optional[str] = Field(description="Firestore document id")

    @validator("year")
    def set_doc_id(cls, year, values):
        values["doc_id"] = str(year)
        return year

    def get_marketplace(self) -> Optional[Marketplace]:
        marketplace = None
        if self.is_fs_col_path_set():
            for marketplace_ in Marketplace.to_list():
                if marketplace_ in self._fs_col_path.split("_")[0]:
                    marketplace = marketplace_
        return marketplace if marketplace == None else Marketplace(marketplace)

    def sync_bsr_category(self):
        # try to extract marketplace from fs col path
        marketplace = self.get_marketplace()
        if marketplace:
            for date_str, bsr in self.bsr.items():
                if not date_str in self.bsr_category:
                    self.bsr_category[date_str] = get_default_category_name(marketplace)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.sync_bsr_category()

    def get_bsr_dict_filtered_by_categories(self, bsr_categories: Optional[List[str]]=None) -> Dict[str, int]:
        """ get bsr if they match bsr_categories. If bsr_categories not set, default top categories by marketplace are used.
        """
        marketplace = self.get_marketplace()
        bsr_categories = bsr_categories if bsr_categories else get_bsr_top_category_names_list(marketplace)
        bsr_dict_filtered = {}
        for (date_str, bsr), (date_str, bsr_cat) in zip(self.bsr.items(), self.bsr_category.items()):
            if bsr_cat in bsr_categories:
                bsr_dict_filtered[date_str] = bsr
        return bsr_dict_filtered

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
    #doc_dict: Optional[Dict[str, FSWatchItemSubCollectionPlotDataYear]] = Field({})# str/key is year

    @classmethod
    def get_subcollection_col_name(cls):
        return "plot_data"

    @classmethod
    def get_subcollection_docs_pydantic_cls(cls) -> Type[FSDocument]:
        return FSWatchItemSubCollectionPlotDataYear

    def get_all_bsr_data(self):
        all_bsr_data = {}
        for year, plot_data in self.items():
            all_bsr_data.update(plot_data.bsr)
        return all_bsr_data

    def get_all_price_data(self):
        all_price_data = {}
        for year, plot_data in self.items():
            all_price_data.update(plot_data.prices)
        return all_price_data

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
    scores_short: Optional[Dict[str, float]] = Field(None, description="Dict with date as key and review score mean in float as value. First value is most in past last closest to present") # str is date_str

    # deprcated, but used in frontend
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
    bsr_last: int = Field(description="Last bsr known. If not exists value is set to self.get_bsr_to_high_to_filter_value(). Takedown shirts should keep their last bsr")
    bsr_first: Optional[int] = None
    bsr_max: Optional[int] = None
    bsr_min: Optional[int] = None
    #bsr_mean: int
    bsr_category: str = Field(description="Matching top category by amazon to bsr data")
    # bsr_last_change: int
    bsr_change: Union[int, float] = Field(description="Bsr change of last month. Is used for merchwatch watch page sorting by strongest change.")
    # bsr_change_total: Optional[Union[int, float]] = None
    bsr_last_range: int = Field(description="Small Integer representation of bsr_last. 0 means bsr_last is between 0-100.000. Every increments is a 100000 range. 99 if higher than 5.000.000")
    bsr_count: int

    def get_bsr_to_high_to_filter_value(self):
        return 999999999

    def inc_bsr_count(self):
        self.bsr_count += 1
        return self

    @staticmethod
    def bsr2bsr_last_range(bsr: int):
        return int(bsr / 100000) if bsr < 5000000 else 99

    @validator("bsr_last_range", always=True)
    def set_bsr_last_range(cls, bsr_last_range, values):
        # 99 stands for higher than max filterable value, i.e. 50.000.000
        return bsr_last_range if bsr_last_range else cls.bsr2bsr_last_range(values["bsr_last"])

    @validator("bsr_first", always=True)
    def set_bsr_first(cls, bsr_first, values):
        return bsr_first if bsr_first else values["bsr_last"]

    @validator("bsr_max", always=True)
    def set_bsr_max(cls, bsr_max, values):
        return bsr_max if bsr_max and bsr_max > values["bsr_last"] else values["bsr_last"]

    @validator("bsr_min", always=True)
    def set_bsr_min(cls, bsr_min, values):
        return bsr_min if bsr_min and bsr_min < values["bsr_last"] else values["bsr_last"]

class FSPriceData(MWBaseModel):
    price_last: float
    price_max: Optional[float] = Field(description="Highest price ever found. Equals to price_last if not set already")
    price_min: Optional[float] = Field(description="Lowest price ever found. Equals to price_last if not set already")
    price_first: Optional[float] = Field(description="Equals to price_last if not set already")
    price_change: float
    price_last_range: int

    #price_mean: float
    #price_last_ranges_array: List[int] = Field(description="Was before used for FS filtering. List of price range indication e.g. 1321 means price is between 13 and 21.")

    @validator("price_first", always=True)
    def set_price_first(cls, price_first, values):
        return price_first if price_first else values["price_last"]

    @validator("price_max", always=True)
    def set_price_max(cls, price_max, values):
        return price_max if price_max and price_max > values["price_last"] and int(values["price_last"]) != 404 else values["price_last"]

    @validator("price_min", always=True)
    def set_price_min(cls, price_min, values):
        return price_min if price_min and price_min < values["price_last"] else values["price_last"]

class FSImageData(MWBaseModel):
    # TODO: make image urls only required if takedown == False
    url_mba_lowq: str
    url_image_q2: str
    url_image_q3: str
    url_image_q4: str
    url_mba_hq: str
    url: Optional[str] = Field(None, description="Http url to private stored image")

class FSKeywordData(MWBaseModel):
    keywords_stem: Dict[str, bool] = Field(description="Important for search alogrithm. Must be set. Keys are keywords stemmed with SnowballStemmer")

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
    # Try to remove everything that is not mandatory to safe in FS
    #upload_since_days: Optional[int] = None
    #upload_since_days_map: Optional[Dict[str, bool]] = None
    #time_since_upload: Optional[float] = None

    def get_days_since_upload(self):
        upload_date_dt = self.upload_date.replace(tzinfo=None) if type(self.upload_date) == datetime else datetime.combine(self.upload_date, datetime.min.time()).replace(tzinfo=None)
        return (datetime.now() - upload_date_dt).days

class FSTrendData(MWBaseModel):
    trend_nr: int = Field(description="trend_nr is to top n ranking index if documents would be sorted by trend")
    trend: float = Field(description="Float trend value calculated by trend formular. Take upload date and bsr into account")
    trend_change: int = Field(description="trend_nr new - trend_nr old. Negativ/low values are better since ranking is better than before.")

class FSScoreData(MWBaseModel):
    score_count: int = 0
    score_last: Optional[float] = None
    score_last_rounded: Optional[int] = Field(None, description="int(round(score_last, 0))")

class FSMBAShirt(FSDocument, FSWatchItemShortenedPlotData, FSBSRData, FSPriceData, FSImageData, FSKeywordData, FSUploadData, FSTrendData, FSScoreData):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    _add_timestamp: bool = PrivateAttr(True)
    _fs_subcollections: Dict[str, Union[FSWatchItemSubCollectionPlotData]] = PrivateAttr({})
    marketplace: Optional[Marketplace] = Field(None, description="Currently not existent in FS, but if known it can be provided to directly set fs_col_path")
    asin: str

    title: str
    brand: str

    is_trademarked: bool = Field(False, description="Whether product is trademarked. If True they can be filtered")

    takedown: bool
    takedown_date: Optional[date] = None


    language: Optional[str] = None
    timestamp: datetime
    time_since_upload_power: Optional[float] = None
    update_last: date
    url_affiliate: Optional[str] = None
    img_affiliate: Optional[str] = None
    affiliate_exists: Optional[bool] = None

    @staticmethod
    def get_change_total(current, previous):
        current = float(current)
        previous = float(previous)
        try:
            return current - previous
        except ZeroDivisionError:
            return 0

    def get_date_str2data_dict(self, data_type) -> Dict[str,Union[float, int]]:
        plot_data_col_name = FSWatchItemSubCollectionPlotData.get_subcollection_col_name()
        if data_type == "bsr":
            # get data from subcollection
            if plot_data_col_name in self._fs_subcollections:
                return self._fs_subcollections[plot_data_col_name].get_all_bsr_data()
            # get data from shortened data
            else:
                return self.bsr_short
        elif data_type == "price":
            if plot_data_col_name in self._fs_subcollections:
                return self._fs_subcollections[plot_data_col_name].get_all_price_data()
            # get data from shortened data
            else:
                return self.prices_short
        else:
            raise NotImplementedError

    def calculate_data_change(self, data_type, days=30):
        """ If subcollections exists take all bsr data to update bsr_change, otherwise take shortened bsr_short plot data
        """
        assert data_type in ["bsr", "price"]

        data_dict = self.get_date_str2data_dict(data_type)
        data_last = self.bsr_last if data_type == "bsr" else self.price_last
        # if no bsr data exists change is expected to be zero
        if not data_dict:
            return 0

        # get datetime object n_days before last bsr date
        last_bsr_date_str = list(collections.OrderedDict(sorted(data_dict.items(), reverse=True)).keys())[0]
        date_n_days_ago = datetime.strptime(str(last_bsr_date_str), '%Y-%m-%d') - timedelta(days=days)

        # key is distance (int) value is date_str (if distance higher than 0 [which means at least n days ago not less])
        distance_to_date_n_days_ago_dict = {(date_n_days_ago - datetime.strptime(str(date_str), '%Y-%m-%d')).days: date_str for date_str, bsr in data_dict.items() if (date_n_days_ago - datetime.strptime(str(date_str), '%Y-%m-%d')).days >= 0}
        if not distance_to_date_n_days_ago_dict:
            # if no value exists with more days than provided days param take closest value, i.e. last of sorted values with distance < 0
            distance_to_date_n_days_ago_dict = {
                (date_n_days_ago - datetime.strptime(str(date_str), '%Y-%m-%d')).days: date_str for date_str, bsr in
                data_dict.items() if (date_n_days_ago - datetime.strptime(str(date_str), '%Y-%m-%d')).days < 0}
            return int(self.get_change_total(data_last, data_dict[list(collections.OrderedDict(sorted(distance_to_date_n_days_ago_dict.items(), reverse=False)).values())[-1]]))

        # sort dict by value (distance) and get first element value (date_str with shortest distance to date_n_days_ago
        data_n_days_ago = data_dict[list(collections.OrderedDict(sorted(distance_to_date_n_days_ago_dict.items(), reverse=False)).values())[0]]
        return int(self.get_change_total(data_last, data_n_days_ago if data_n_days_ago else data_last))

    def update_bsr_change(self):
        self.bsr_change = self.calculate_data_change("bsr", days=30)
        return self

    def update_short_dicts(self, bsr_last: Optional[int]=None, price_last: Optional[float]=None):
        """
            bsr_last: last bsr (today crawled). If not set it is expected, that latest bsr data is set in subcollection data
        """
        today_date_str = str(datetime.now().date())
        is_plotdata_subcol_set = self.are_subcollections_set(fs_subcol_cls_list=[FSWatchItemSubCollectionPlotData])
        # if no subcollections set, just append bsr_last to bsr_short dict
        if not is_plotdata_subcol_set and bsr_last and bsr_last != 404 and (not self.bsr_short or today_date_str not in self.bsr_short):
            if not self.bsr_short:
                self.bsr_short = {today_date_str: bsr_last}
            else:
                self.bsr_short[today_date_str] = bsr_last
        if not is_plotdata_subcol_set and price_last != None and int(price_last) != 404 and (not self.prices_short or today_date_str not in self.prices_short):
            # if either no prices_short data exists or current last element is different from provided price_last
            if not self.prices_short:
                self.prices_short = {today_date_str: price_last}
            elif self.prices_short[sorted(list(self.prices_short.keys()))[-1]] != price_last:
                self.prices_short[today_date_str] = price_last
        # if subcollections are set use all bsr data and calculate a new bsr_short with rdp
        if is_plotdata_subcol_set:
            sub_collection_dict = copy.deepcopy(self._fs_subcollections)
            for year_str, fs_doc in sub_collection_dict[FSWatchItemSubCollectionPlotData.get_subcollection_col_name()].items():
                fs_doc.bsr = fs_doc.get_bsr_dict_filtered_by_categories()
            self.update(get_shortened_plot_data(sub_collection_dict, max_number_of_plot_points=20, min_number_of_plot_points=18))
        return self

    def sync_shortened_dicts2subcollections(self):
        """ Sync shortened data like bsr_short to subcollections, so that subcollections contain at least short data
            TODO: Threat of droping existing subcol data in FS for older years than current year. If only one document would exist in local subcollections  e.g. 2022, 2020 in FS would be overwritten with short data
        """
        for data_type, short_dict in {"bsr": self.bsr_short, "prices": self.prices_short, "scores": self.scores_short}.items():
            if type(short_dict) == dict:
                for date_str, data in short_dict.items():
                    data_dt = datetime.strptime(str(date_str), '%Y-%m-%d')
                    if data_type=="bsr":
                        self.update_plot_data_subcollection(bsr_last=data, data_dt=data_dt)
                    if data_type=="prices":
                        self.update_plot_data_subcollection(price_last=data, data_dt=data_dt)
                    if data_type=="scores":
                        self.update_plot_data_subcollection(score_last=data, data_dt=data_dt)
        return self

    def update_plot_data_subcollection(self, bsr_last=None, bsr_category=None, price_last=None, score_last=None, data_dt: Optional[datetime]=None):
        """ This functions creates a plot data subcollection if it not exists or updates data with provided bsr, price or score data
            Important: It is expected that data was crawled today. If not please provide data_dt
        """
        # if subcollections are not set, try first to get them from FS (but only last existing document)
        fs_subcol_cls = FSWatchItemSubCollectionPlotData
        if not self.are_subcollections_set(fs_subcol_cls_list=[fs_subcol_cls]):
            self.update_fs_subcollections(
                fs_subcol_cls.parse_fs_col_path(f"{self.get_fs_col_path()}/{self.doc_id}/{fs_subcol_cls.get_subcollection_col_name()}",
                                                get_docs_settings=GetFSDocsSettings(limit=1, order_by="year", order_by_direction=OrderByDirection.DESC)))

        data_dt = data_dt if data_dt else datetime.now()
        year_str: str = str(data_dt.year)
        date_today_str: str = str(data_dt.date())
        plotdata_col_name = fs_subcol_cls.get_subcollection_col_name()
        # case no plot data exists for year_current
        if not self._fs_subcollections or (plotdata_col_name in self._fs_subcollections and year_str not in self._fs_subcollections[plotdata_col_name]):
            fs_subcol_doc = FSWatchItemSubCollectionPlotDataYear(year=year_str, doc_id=year_str,
                                                 bsr={date_today_str: bsr_last} if bsr_last != None else {},
                                                 bsr_category={date_today_str: bsr_category} if bsr_category != None else {},
                                                 prices={date_today_str: price_last} if price_last != None else {},
                                                 scores={date_today_str: score_last} if score_last != None else {})
            fs_subcol_doc.set_fs_col_path(f"{self.get_fs_col_path()}/{self.doc_id}/{fs_subcol_cls.get_subcollection_col_name()}")
            if not self._fs_subcollections:
                self._fs_subcollections = {plotdata_col_name: fs_subcol_cls.parse_obj({fs_subcol_doc.doc_id: fs_subcol_doc.dict()})}
            elif year_str not in self._fs_subcollections[plotdata_col_name]:
                self._fs_subcollections[plotdata_col_name].update_doc_dict(fs_subcol_doc)
        # case data exists already and should be extended
        else:
            if bsr_last:
                self._fs_subcollections[plotdata_col_name][year_str].bsr[date_today_str] = bsr_last
            if bsr_category:
                self._fs_subcollections[plotdata_col_name][year_str].bsr_category[date_today_str] = bsr_category
            if price_last:
                self._fs_subcollections[plotdata_col_name][year_str].prices[date_today_str] = price_last
            if score_last:
                self._fs_subcollections[plotdata_col_name][year_str].scores[date_today_str] = score_last

    def update_bsr_data(self, bsr_last: Optional[int]=None, bsr_category: Optional[str]=None):
        # Function to update bsr data with NEW bsr_last
        if bsr_last != 404 and bsr_last != self.bsr_last and bsr_last != 0 and bsr_last != None:
            # What should happen if bsr_category is different from before?
            # 28.12.21: Add them also to subcol and filter afterwards
            self.bsr_last = bsr_last
            self.bsr_category = bsr_category
            #self.sync_shortened_dicts2subcollections() # use it only with care (threat of removing existing data in FS)
            self.update_plot_data_subcollection(bsr_last=bsr_last, bsr_category=bsr_category) # reads subcol data (at least one document if existend)
            self.inc_bsr_count()
            # update bsr last range
            self.bsr_last_range = self.bsr2bsr_last_range(self.bsr_last)
            # update bsr_min and max
            if not self.bsr_min or (self.bsr_last < self.bsr_min):
                self.bsr_min = self.bsr_last
            if not self.bsr_max or (self.bsr_last > self.bsr_max):
                self.bsr_max = self.bsr_last
            return self.update_bsr_change()
        return self

    def update_price_data(self, price_last: Optional[float]=None):
        # exlucde update with unreal price data
        if price_last > 0 and price_last < 404 and price_last != None and price_last != self.price_last:
            self.price_last = price_last
            self.update_plot_data_subcollection(price_last=price_last)
            # update price_max
            if not self.price_max or (self.price_last > self.price_max):
                self.price_max = self.price_last
            self.price_last_range = int(self.price_last)
            return self.update_price_change()
        return self

    def update_price_change(self):
        self.price_change = self.calculate_data_change("price", days=30)
        return self

    def update_score_data(self, score_last: Optional[float]=None, score_count: Optional[int]=None):
        if type(score_last) == float and score_last > 0:
            self.score_last = score_last
            self.score_last_rounded = int(round(score_last, 0))
            self.update_plot_data_subcollection(score_last=score_last)
        if score_count:
            self.score_count = score_count
        return self

    def calculate_trend(self, month_decreasing_trend=6):
        """ Trend score is low (better) if days since uplaod are low and bsr_last
            if upload is longer in past than month_decreasing_trend, trend score should be higher
        """
        return get_trend_multiplicator(self.get_days_since_upload()) * self.bsr_last

    def update_trend_value(self):
        trend_old = copy.deepcopy(self.trend)
        self.trend = self.calculate_trend()
        self.trend_change = self.trend - trend_old
        return self

    def update_data(self, bsr_last: Optional[int]=None, bsr_category: Optional[str]=None, price_last: Optional[float]=None, score_last: Optional[float]=None, score_count: Optional[int]=None):
        # TODO: Update keyword data (splitted in subcategories)
        return self.update_bsr_data(bsr_last, bsr_category).update_price_data(price_last).update_score_data(score_last,score_count).update_short_dicts().update_trend_value()

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

        fields_included = {"asin", "bsr_short", "prices_short", "bsr_change", "bsr_mean", "bsr_last", "keywords_meaningful", "url", "url_affiliate", "url_mba_hq", "url_mba_lowq", "url_image_q2", "url_image_q3", "url_image_q4", "price_last", "update_last", "img_affiliate", "title", "brand", "trend", "trend_nr", "trend_change", "upload_date", "takedown", "takedown_date"}
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

    def extract_marketplace_by_fs_col_path(self) -> Optional[Marketplace]:
        for marketplace in Marketplace.to_list():
            if marketplace in self._fs_col_path:
                return Marketplace(marketplace)
        return None

    def set_marketplace(self):
        self.marketplace = self.marketplace if self.marketplace else self.extract_marketplace_by_fs_col_path()

    def set_fields_of_not_comparable_mba_shirt(self):
        self.bsr_last = self.get_bsr_to_high_to_filter_value()
        # TODO: prevent bsr_short and bsr value in history/subcollection to be set

    def write_to_firestore(self, exclude_doc_id=False, exclude_fields=["plot_x", "plot_y", "plot_x_price", "plot_y_price"], overwrite_doc=None, array_union=None, write_subcollections=True, client=None):
        super().write_to_firestore(exclude_doc_id=exclude_doc_id, exclude_fields=exclude_fields, overwrite_doc=overwrite_doc, array_union=array_union,write_subcollections=write_subcollections, client=client)


    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.set_marketplace()
        # if bsr_category is not comparable with other mba shirts, set bsr_last to 999999999
        if self.bsr_category not in get_bsr_top_category_names_list(self.marketplace):
            print(f"Warning: Shirt with asin {self.asin} does not contain a comparable bsr category, but {self.bsr_category} for marketplace {self.marketplace}")
            self.set_fields_of_not_comparable_mba_shirt()


class MBAShirtOrderByField(str, EnumBase):
    # value is FS field
    BSR="bsr_last"
    PRICE="price_last"
    TREND="trend"
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


def get_default_category_name(marketplace) -> str:
    if marketplace == "de":
        return "Fashion"
    else:
        return "Clothing, Shoes & Jewelry"


def get_bsr_top_category_names_list(marketplace):
    if marketplace == "de":
        return ["Fashion", "Bekleidung"]
    else:
        return ["Clothing, Shoes & Jewelry"]

def get_bsr_category(array_bsr_categorie_str: str, marketplace):
    # array_bsr_categorie_str e.g. "['Spielzeug', 'Schultüten']"
    if marketplace == "de":
        try:
            bsr_category = array_bsr_categorie_str.strip("[]").split(",")[
                0].strip("'")
        except Exception as e:
            print(array_bsr_categorie_str)
            print("Could not extract bsr_category", str(e))
            bsr_category = ""
    else:
        # does not split "," which does not work for "Clothing, Shoes & Jewelry"
        try:
            bsr_category = re.findall(
                "'([^']*)'", array_bsr_categorie_str.strip("[]"))[0]
        except Exception as e:
            print(array_bsr_categorie_str)
            print("Could not extract bsr_category", str(e))
            bsr_category = ""
    if bsr_category == "404" or bsr_category == "":
        bsr_category = get_default_category_name(marketplace)
    return bsr_category


#t = FSMBAShirt.parse_fs_doc_snapshot(get_document_snapshot("/de_shirts/B089SKWDJN"), read_subcollections=[FSWatchItemSubCollectionPlotData], read_subcollection_docs_settings_dict={FSWatchItemSubCollectionPlotData:GetFSDocsSettings(limit=2, order_by="year", order_by_direction=OrderByDirection.DESC)})
#t.update_data(bsr_last=12312, bsr_category="Schuhe", price_last=22.24, score_last=4.6)
#t.write_to_firestore()
#t.set_bsr_change()
#test = 0

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