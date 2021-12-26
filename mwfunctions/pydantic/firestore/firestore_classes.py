from __future__ import annotations

from typing import Dict, Optional, List, Any
from abc import ABC

from pydantic import Field, PrivateAttr
from datetime import date
from google.cloud.firestore import DocumentSnapshot
from mwfunctions.pydantic.base_classes import MWBaseModel
from mwfunctions.cloud.firestore import get_docs_snap_iterator


def date2str(dict_obj):
    # transform date values to strings, because FS cant store date format (only datetime or string)
    for key, value in dict_obj.items():
        if isinstance(value, date):
            dict_obj[key] = str(value)

class FSDocument(MWBaseModel):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    ## private fields
    _fs_col_path: str = PrivateAttr() # Full collection path under which doc_id can be found in Firestore
    _fs_subcollections: Dict[str, FSSubcollection] = PrivateAttr({}) # str is col_name of FSSubcollection

    # write to FS default settings
    _overwrite_doc: bool = PrivateAttr(False)
    _array_union: bool = PrivateAttr(False)

    ## public fields
    doc_id: str = Field(description="Firestore document id")

    @classmethod
    def parse_fs_doc_snapshot(cls, doc_snap: DocumentSnapshot, read_subcollections: List[type[FSSubcollection]]=None, max_number_subcollections=None) -> FSDocument:
        """ Takes a doc_snapshot and parses data to FSDOcument object
            if read_subcollections:
                all subcollections and all documents within subcollections are readed, too. Default is False, to prevent read costs
            max_number_subcollections (int): if provided only n number collections are read from FS
        """
        fs_col_path = "/".join(doc_snap.reference.path.split("/")[0:-1])
        fs_doc: FSDocument = cls.parse_obj({"doc_id": doc_snap.id, **doc_snap.to_dict(), "_fs_col_path": fs_col_path})#.set_fs_col_path(fs_col_path)
        if read_subcollections:
            for subcollection_ref in doc_snap.reference.collections(page_size=max_number_subcollections):
                # TODO: test this code
                # add subcollection to self._fs_subcollections by FSSubcollection object
                # TODO: provide pydantic of subcollection doc somehow..
                matching_subcollection_classes = [fs_sub_cls for fs_sub_cls in read_subcollections if subcollection_ref.id == fs_sub_cls.get_subcollection_col_name()]
                if len(matching_subcollection_classes) > 0:
                    fs_doc.update_fs_subcollections(matching_subcollection_classes[0].parse_fs_col_path(f"{subcollection_ref.parent.path}/{subcollection_ref.id}"))
        return fs_doc

    def set_fs_col_path(self, fs_col_path) -> FSDocument:
        self._fs_col_path = fs_col_path
        return self

    def get_fs_col_path(self):
        return self._fs_col_path

    def is_fs_col_path_set(self):
        try:
            return bool(self._fs_col_path)
        except Exception:
            return False

    def get_fs_doc_path(self):
        return f"{self._fs_col_path}/{self.doc_id}"

    def update_fs_subcollections(self, subcollection: FSSubcollection):
        # update subcollection of fs_document and sets all _fs_col_path of all documents within subcollection
        for doc_id, subcollection_doc in subcollection.doc_dict.items():
            if not subcollection_doc.is_fs_col_path_set():
                subcollection_doc.set_fs_col_path(f"{self._fs_col_path}/{self.doc_id}/{subcollection.get_subcollection_col_name()}")
        self._fs_subcollections[subcollection.get_subcollection_col_name()] = subcollection

    def write_to_firestore(self, exclude_doc_id=False, exclude_fields=[], overwrite_doc=None, array_union=None, write_subcollections=True, client=None):
        """ Writes pydantic object to FS
        """
        # load module in function to prevent circular import
        from mwfunctions.cloud.firestore import write_document_dict

        exclude_fields = exclude_fields + ["doc_id"] if exclude_doc_id else exclude_fields
        dict_to_fs = self.dict(exclude=set(exclude_fields))
        date2str(dict_to_fs)
        write_document_dict(dict_to_fs, f"{self._fs_col_path}/{self.doc_id}", array_union=array_union if array_union != None else self._array_union, overwrite_doc=overwrite_doc if overwrite_doc != None else self._overwrite_doc, client=client)
        if write_subcollections:
            for col_name, fs_subcollection in self._fs_subcollections.items():
                for doc_id, fs_document in fs_subcollection.doc_dict.items():
                    fs_document.write_to_firestore(exclude_doc_id=exclude_doc_id, exclude_fields=exclude_fields, write_subcollections=write_subcollections, client=client)


    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        if "_fs_col_path" in data:
            self.set_fs_col_path(data["_fs_col_path"])

        #
    # def update_fs_subcollections(self, col_name, subcollection_doc: FSDocument):
    #     subcollection_doc.set_fs_col_path(f"{self._fs_col_path}/{self.doc_id}/{col_name}")
    #     if col_name not in self._fs_subcollections:
    #         self._fs_subcollections[col_name] = [subcollection_doc]
    #     else:
    #         self._fs_subcollections[col_name].append(subcollection_doc)

class FSSubcollection(MWBaseModel):
    doc_dict: Optional[Dict[str, FSDocument]] = Field({}, description="All documents contained in subcollection. key of dict is doc_id of FSDocument")

    @classmethod
    def parse_fs_col_path(cls, fs_col_path, read_subcollections: List[type[FSSubcollection]]=None, client=None) -> FSSubcollection:
        """ Takes a FS collection path in format col/doc/col/.../col (odd number of path elements) and parses all documents to fs_doc_pydantic_class object which is filled to doc_dict
        """
        doc_dict = {}
        for doc_snap in get_docs_snap_iterator(fs_col_path, client=client):
            fs_doc: FSDocument = cls.get_subcollection_docs_pydantic_cls().parse_fs_doc_snapshot(doc_snap, read_subcollections=read_subcollections)
            doc_dict[doc_snap.id] = fs_doc.dict()

        return cls(doc_dict=doc_dict)

    @classmethod
    def get_subcollection_col_name(cls) -> str:
        # ABSTRACT method must be defined by childs
        # col name of sub collection e.g. plot_data
        raise NotImplementedError

    @classmethod
    def get_subcollection_docs_pydantic_cls(cls) -> type[FSDocument]:
        # returns class FSDocument for typing Optional[Dict[str, FSDocument]]
        return cls.__dict__["__fields__"]["doc_dict"].sub_fields[0].outer_type_

    def update_doc_dict(self, subcollection_doc: FSDocument):
        self.doc_dict[subcollection_doc.doc_id] = subcollection_doc


