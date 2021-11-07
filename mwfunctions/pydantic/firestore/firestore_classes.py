from __future__ import annotations
from typing import Union, Dict, List, Optional, Any
from pydantic import BaseModel, Field, PrivateAttr, validator
from mwfunctions.pydantic.base_classes import MWBaseModel, Marketplace
from mwfunctions.cloud.firestore import split_correct_path, write_document_dict

class FSDocument(MWBaseModel):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    _fs_col_path: str = PrivateAttr()
    _fs_subcollections: Dict[str, FSSubcollection] = PrivateAttr({}) # str is subcollection_col_name of FSSubcollection
    _overwrite_doc: bool = PrivateAttr(False)
    _array_union: bool = PrivateAttr(False)
    doc_id: str = Field(description="Firestore document id")

    def set_fs_col_path(self, fs_col_path):
        self._fs_col_path = fs_col_path

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

class FSWatchItemSubCollectionDict(FSSubcollection):
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
    plot_data: Optional[Dict[str, FSWatchItemSubCollectionPlotDataYear]] = Field({}) # str/key is year

    @validator("plot_data")
    def set_plot_data(cls, plot_data, values):
        # backwards comp
        values["subcollection_doc_dict"] = plot_data
        return plot_data

    @validator("subcollection_doc_dict")
    def set_subcollection_doc_dict(cls, subcollection_doc_dict, values):
        # backwards comp
        values["plot_data"] = subcollection_doc_dict
        return subcollection_doc_dict

class FSWatchItemShortenedPlotData(MWBaseModel):
    """ Example:
            {
                "bsr_short":
                    {"2020-09-20": 480549, ...}
                "prices_short":
                    {"2020-09-20": 480549, ...}
            }
    """
    bsr_short: Dict[str, Union[int, float]] # str is date_str
    prices_short: Dict[str, Union[int, float]] # str is date_str


class FSMBAShirt(FSDocument, FSWatchItemShortenedPlotData):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    _fs_subcollections: Dict[str, Union[FSWatchItemSubCollectionDict]] = PrivateAttr({})
    marketplace: Marketplace
    doc_id: str = Field(description="Firestore document id")

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.set_fs_col_path( f"{self.marketplace}_shirts")

