from mwfunctions.pydantic.firestore import FSDocument, FSSubcollection
from mwfunctions.pydantic.crawling_classes import CrawlingType
from mwfunctions.pydantic.base_classes import MWBaseModel, Marketplace

from typing import Union, Dict, List, Optional, Any
from pydantic import BaseModel, Field, PrivateAttr, validator
from enum import Enum
from datetime import datetime

"""
    This file defines crawling product logs.
    For example if an overview product was successfully crawled it should be logged in Firestore (logging means that a document with ASIN is created)
    
"""

class FSMBACrawlingProductLogsSubcollectionDoc(FSDocument):
    _overwrite_doc: bool = PrivateAttr(True)
    doc_id: str = Field(description="Doc id is a ASIN")
    create_timestamp: datetime = Field(datetime.now())

    def update_timestamp(self):
        self.create_timestamp = datetime.now()

class FSMBACrawlingProductLogsSubcollection(FSSubcollection):
    subcollection_col_name: CrawlingType
    subcollection_doc_dict: Optional[Dict[str, FSMBACrawlingProductLogsSubcollectionDoc]] = Field({}, description="All documents contained in subcollection. key of dict is doc_id of FSDocument")

class FSMBACrawlingProductLogs(FSDocument):
    _fs_subcollections: Dict[str, Union[FSMBACrawlingProductLogsSubcollection]] = PrivateAttr({})
    marketplace: Marketplace
    doc_id: Optional[Marketplace] = Field(description="Firestore document id is a Marketplace")

    @validator("marketplace")
    def set_doc_id(cls, marketplace, values):
        values["doc_id"] = str(marketplace)
        return marketplace

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self.set_fs_col_path(f"crawling_product_logs")