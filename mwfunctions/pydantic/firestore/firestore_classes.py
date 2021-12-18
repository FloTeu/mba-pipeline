from __future__ import annotations

from typing import Dict, Optional

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
    _fs_col_path: str = PrivateAttr() # Full collection path under which doc_id can be foundin Firestore
    _fs_subcollections: Dict[str, FSSubcollection] = PrivateAttr({}) # str is col_name of FSSubcollection
    # write to FS settings
    _overwrite_doc: bool = PrivateAttr(False)
    _array_union: bool = PrivateAttr(False)

    ## public fields
    doc_id: str = Field(description="Firestore document id")

    @classmethod
    def parse_fs_doc_snapshot(cls, doc_snap: DocumentSnapshot, read_subcollections=False, max_number_subcollections=None) -> FSDocument:
        """ Takes a doc_snapshot and parses data to FSDOcument object
            if read_subcollections:
                all subcollections and all documents within subcollections are readed, too. Default is False, to prevent read costs
            max_number_subcollections (int): if provided only n number collections are read from FS
        """
        fs_col_path = "/".join(doc_snap.reference.path.split("/")[0:-1])
        fs_doc: FSDocument = cls.parse_obj({"doc_id": doc_snap.id, **doc_snap.to_dict()}).set_fs_col_path(fs_col_path)
        if read_subcollections:
            for subcollection_ref in doc_snap.reference.collections(page_size=max_number_subcollections):
                # TODO: test this code
                # add subcollection to self._fs_subcollections by FSSubcollection object
                fs_doc.update_fs_subcollections(FSSubcollection.parse_fs_col_path(subcollection_ref.path, read_subcollections=True))
        return fs_doc

    def set_fs_col_path(self, fs_col_path) -> FSDocument:
        self._fs_col_path = fs_col_path
        return self

    def get_fs_col_path(self):
        return self._fs_col_path

    def is_fs_col_path_set(self):
        return bool(self._fs_col_path)

    def get_fs_doc_path(self):
        return f"{self._fs_col_path}/{self.doc_id}"

    def update_fs_subcollections(self, subcollection: FSSubcollection):
        # update subcollection of fs_document and sets all _fs_col_path of all documents within subcollection
        for doc_id, subcollection_doc in subcollection.doc_dict.items():
            if not subcollection_doc.is_fs_col_path_set():
                subcollection_doc.set_fs_col_path(f"{self._fs_col_path}/{self.doc_id}/{subcollection.col_name}")
        self._fs_subcollections[subcollection.col_name] = subcollection

    def write_to_firestore(self, exclude_doc_id=False, exclude_fields=[], write_subcollections=True, client=None):
        # load module in function to prevent circular import
        from mwfunctions.cloud.firestore import write_document_dict

        exclude_fields = exclude_fields + ["doc_id"] if exclude_doc_id else exclude_fields
        dict_to_fs = self.dict(exclude=set(exclude_fields))
        date2str(dict_to_fs)
        write_document_dict(dict_to_fs, f"{self._fs_col_path}/{self.doc_id}", array_union=self._array_union, overwrite_doc=self._overwrite_doc, client=client)
        if write_subcollections:
            for col_name, fs_subcollection in self._fs_subcollections.items():
                for doc_id, fs_document in fs_subcollection.doc_dict.items():
                    fs_document.write_to_firestore(exclude_doc_id=exclude_doc_id, exclude_fields=exclude_fields, write_subcollections=write_subcollections, client=client)

    #
    # def update_fs_subcollections(self, col_name, subcollection_doc: FSDocument):
    #     subcollection_doc.set_fs_col_path(f"{self._fs_col_path}/{self.doc_id}/{col_name}")
    #     if col_name not in self._fs_subcollections:
    #         self._fs_subcollections[col_name] = [subcollection_doc]
    #     else:
    #         self._fs_subcollections[col_name].append(subcollection_doc)

class FSSubcollection(MWBaseModel):
    col_name: str = Field(description="col name of sub collection e.g. plot_data")
    doc_dict: Optional[Dict[str, FSDocument]] = Field({}, description="All documents contained in subcollection. key of dict is doc_id of FSDocument")

    @classmethod
    def parse_fs_col_path(cls, fs_col_path, fs_doc_pydantic_class: FSDocument, client=None, read_subcollections=False) -> FSSubcollection:
        """ Takes a FS collection path in format col/doc/col/.../col (odd number of path elements) and parses all documents to fs_doc_pydantic_class object which is filled to doc_dict
        """
        doc_dict = {}
        col_name = fs_col_path.split("/")[-1]
        for doc_snap in get_docs_snap_iterator(fs_col_path, client=client):
            fs_doc: FSDocument = fs_doc_pydantic_class.parse_fs_doc_snapshot(doc_snap, read_subcollections=read_subcollections)
            doc_dict[doc_snap.id] = fs_doc

        return cls.parse_obj({"col_name": col_name, "doc_dict": doc_dict})

    def update_doc_dict(self, subcollection_doc: FSDocument):
        self.doc_dict[subcollection_doc.doc_id] = subcollection_doc


