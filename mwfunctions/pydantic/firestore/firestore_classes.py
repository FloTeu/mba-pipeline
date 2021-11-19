from __future__ import annotations

import copy
from typing import Union, Dict, List, Optional, Any
from pydantic import BaseModel, Field, PrivateAttr, validator
from datetime import date, datetime
from google.cloud.firestore import DocumentSnapshot
from mwfunctions.pydantic.base_classes import MWBaseModel, Marketplace


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
        write_document_dict(self.dict(exclude=set(exclude_fields)), f"{self._fs_col_path}/{self.doc_id}", array_union=self._array_union, overwrite_doc=self._overwrite_doc, client=client)
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
    # init deprecated values first, because they might be needed to set bsr_short for older documents
    plot_x: Optional[List[str]] = Field(None, description="Deprecated. List of x axis data, which are dates in '%d/%m/%Y' format")
    plot_y: Optional[List[str]] = Field(None, description="Deprecated. List of y axis data, which is a list of bsr")
    plot_x_price: Optional[List[str]] = Field(None, description="Deprecated. List of x axis data, which are dates in '%d/%m/%Y' format")
    plot_y_price: Optional[List[str]] = Field(None, description="Deprecated. List of y axis data, which is a list of prices stringifed from floats")
    # TODO: try to change format of key to date not date_str
    bsr_short: Optional[Dict[str, int]] = Field(None, description="Dict with date as key and bsr as value. First value is most in past last closest to present") # str is date_str
    prices_short: Optional[Dict[str, float]] = Field(None, description="Dict with date as key and price in float as value. First value is most in past last closest to present") # str is date_str

    @validator("bsr_short", always=True)
    def set_bsr_short(cls, bsr_short, values):
        plot_x = copy.deepcopy(values["plot_x"].split(","))
        plot_x.reverse()
        plot_y = copy.deepcopy(values["plot_y"].split(","))
        plot_y.reverse()
        # TODO: try to change format of key to date not date_str
        return bsr_short or {str(datetime.strptime(date_str, "%d/%m/%Y").date()): bsr for date_str, bsr in zip(plot_x,plot_y)}

    @validator("prices_short", always=True)
    def set_prices_short(cls, prices_short, values):
        plot_x_price = copy.deepcopy(values["plot_x_price"].split(","))
        plot_x_price.reverse()
        plot_y_price = copy.deepcopy(values["plot_y_price"].split(","))
        plot_y_price.reverse()
        # TODO: try to change format of key to date not date_str
        return prices_short or {str(datetime.strptime(date_str, "%d/%m/%Y").date()): price for date_str, price in zip(plot_x_price,plot_y_price)}

    # followwing 4 validators:
    # Backwards comp for MW API. Frontend requires a string with comma sep for getting plot data
    # TODO: Only shortened data (new) is created by old data not the other ways around. Not possible with validator unfortunatly. Check if frontend still works with new format
    # TODO: Seems like frontend requires old structure with plot_x, plot_y ...

    @validator("plot_x", always=True)
    def set_plot_x(cls, plot_x, values):
        return ",".join(plot_x) if plot_x else "" #",".join(list(values["bsr_short"].keys()))

    @validator("plot_y", always=True)
    def set_plot_y(cls, plot_y, values):
        return ",".join(plot_y) if plot_y else "" #",".join(list(values["bsr_short"].values()))

    @validator("plot_x_price", always=True)
    def set_plot_x_price(cls, plot_x_price, values):
        return ",".join(plot_x_price) if plot_x_price else "" # ",".join(list(values["prices_short"].keys()))

    @validator("plot_y_price", always=True)
    def set_plot_y_price(cls, plot_y_price, values):
        return ",".join(plot_y_price) if plot_y_price else "" #",".join(list(values["prices_short"].values()))


class FSMBAShirt(FSDocument, FSWatchItemShortenedPlotData):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    _fs_subcollections: Dict[str, Union[FSWatchItemSubCollectionPlotData]] = PrivateAttr({})
    # marketplace: Optional[Marketplace] = Field(None, description="Currently not existent in FS, but if known it can be provided to directly set fs_col_path")
    # TODO: add oher fields of fireste document
    bsr_last: int
    price_last: float
    upload_date: datetime

    # def __init__(self, **data: Any) -> None:
    #     super().__init__(**data)
    #     if self.marketplace:
    #         self.set_fs_col_path( f"{self.marketplace}_shirts")

