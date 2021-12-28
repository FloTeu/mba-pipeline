from __future__ import annotations

from typing import Dict, Optional, List, Any, Type
from abc import ABC

from pydantic import Field, PrivateAttr, Extra
from datetime import date
from google.cloud.firestore import DocumentSnapshot
from mwfunctions.pydantic.base_classes import MWBaseModel
from mwfunctions.cloud.firestore import get_docs_snap_iterator, OrderByDirection, get_docs_batch


def date2str(dict_obj):
    # transform date values to strings, because FS cant store date format (only datetime or string)
    for key, value in dict_obj.items():
        if isinstance(value, date):
            dict_obj[key] = str(value)

class GetFSDocsSettings(MWBaseModel):
    limit: Optional[int]=None
    order_by: Optional[str]=None
    order_by_direction: Optional[OrderByDirection]=OrderByDirection.ASC

class FSDocument(MWBaseModel):
    ''' Child of FSDocument must contain all field values of document to create this document.
    '''
    ## private fields
    _fs_col_path: str = PrivateAttr() # Full collection path under which doc_id can be found in Firestore
    _fs_subcollections: Dict[str, FSSubcollection] = PrivateAttr({}) # str is col_name of FSSubcollection

    # write to FS default settings
    _overwrite_doc: bool = PrivateAttr(False)
    _array_union: bool = PrivateAttr(False)
    _add_timestamp: bool = PrivateAttr(False)

    ## public fields
    doc_id: str = Field(description="Firestore document id")

    @classmethod
    def parse_fs_doc_snapshot(cls, doc_snap: DocumentSnapshot, read_subcollections: Optional[List[Type[FSSubcollection]]]=None, read_subcollection_docs_settings_dict: Optional[Dict[Type[FSSubcollection], GetFSDocsSettings]]=None, max_number_subcollections=None) -> FSDocument:
        """ Takes a doc_snapshot and parses data to FSDOcument object
            if read_subcollections:
                all provided subcollection classes and read_subcollection_n_docs_dict or all documents within subcollections are readed, too. Default is None, to prevent read costs
            max_number_subcollections (int): if provided only n number collections are read from FS
        """
        fs_col_path = "/".join(doc_snap.reference.path.split("/")[0:-1])
        fs_doc: FSDocument = cls.parse_obj({"doc_id": doc_snap.id, **doc_snap.to_dict(), "_fs_col_path": fs_col_path})#.set_fs_col_path(fs_col_path)
        if read_subcollections:
            for subcollection_ref in doc_snap.reference.collections(page_size=max_number_subcollections):
                # add subcollection to self._fs_subcollections by FSSubcollection object
                # TODO: provide pydantic of subcollection doc somehow..
                matching_subcollection_classes = [fs_sub_cls for fs_sub_cls in read_subcollections if subcollection_ref.id == fs_sub_cls.get_subcollection_col_name()]
                if len(matching_subcollection_classes) > 0:
                    fs_subcol_cls = matching_subcollection_classes[0]
                    get_docs_settings = read_subcollection_docs_settings_dict[fs_subcol_cls] if read_subcollection_docs_settings_dict and fs_subcol_cls in read_subcollection_docs_settings_dict else None
                    fs_doc.update_fs_subcollections(fs_subcol_cls.parse_fs_col_path(f"{subcollection_ref.parent.path}/{subcollection_ref.id}", get_docs_settings=get_docs_settings))
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
        for doc_id, subcollection_doc in subcollection.items():
            if not subcollection_doc.is_fs_col_path_set():
                subcollection_doc.set_fs_col_path(f"{self._fs_col_path}/{self.doc_id}/{subcollection.get_subcollection_col_name()}")
        self._fs_subcollections[subcollection.get_subcollection_col_name()] = subcollection

    def write_to_firestore(self, exclude_doc_id=False, exclude_fields=[], overwrite_doc=None, array_union=None, write_subcollections=True, add_timestamp=None, client=None):
        """ Writes pydantic object to FS
        """
        # load module in function to prevent circular import
        from mwfunctions.cloud.firestore import write_document_dict

        exclude_fields = exclude_fields + ["doc_id"] if exclude_doc_id else exclude_fields
        dict_to_fs = self.dict(exclude=set(exclude_fields))
        date2str(dict_to_fs)
        write_document_dict(dict_to_fs, f"{self._fs_col_path}/{self.doc_id}", array_union=array_union if array_union != None else self._array_union, overwrite_doc=overwrite_doc if overwrite_doc != None else self._overwrite_doc, add_timestamp=add_timestamp if add_timestamp!=None else self._add_timestamp, client=client)
        if write_subcollections:
            for col_name, fs_subcollection in self._fs_subcollections.items():
                for doc_id, fs_document in fs_subcollection.items():
                    fs_document.write_to_firestore(exclude_doc_id=exclude_doc_id, exclude_fields=exclude_fields, write_subcollections=write_subcollections, client=client)

    def are_subcollections_set(self, fs_subcol_cls_list: Optional[List[Type[FSSubcollection]]]=None):
        """ Checks if any subcollections are set. If fs_subcol_cls_list are provided only for those sub collections is checked if they are set
        """
        if not fs_subcol_cls_list:
            return bool(self._fs_subcollections) # checks if at least one subcollection is set
        elif not self._fs_subcollections:
            return False # if not any subcol is set always return False
        else:
            return all([fs_subcol_cls.get_subcollection_col_name() in self._fs_subcollections for fs_subcol_cls in fs_subcol_cls_list])


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

class FSSubcollection(MWBaseModel, extra=Extra.allow):
    # fields are
    #doc_dict: Optional[Dict[str, FSDocument]] = Field({}, description="All documents contained in subcollection. key of dict is doc_id of FSDocument")

    # class config:
    #     extra=Extra.allow

    def __init__(self, **data: Any) -> None:
        for field_name, fs_doc in data.items():
            data[str(field_name)] = self.get_subcollection_docs_pydantic_cls().parse_obj(fs_doc)
        super().__init__(**data)

    @classmethod
    def parse_fs_col_path(cls, fs_col_path, get_docs_settings: Optional[GetFSDocsSettings]=None, read_subcollections: List[Type[FSSubcollection]]=None, client=None) -> FSSubcollection:
        """ Takes a FS collection path in format col/doc/col/.../col (odd number of path elements) and parses all documents to fs_doc_pydantic_class object which is filled to doc_dict
           if get_docs_settings set -> limit and order of fs docs can be customized
        """
        get_docs_settings = get_docs_settings if get_docs_settings else GetFSDocsSettings()
        doc_dict = {}
        for doc_snap in get_docs_batch(fs_col_path, limit=get_docs_settings.limit, order_by=get_docs_settings.order_by, direction=get_docs_settings.order_by_direction, client=client):
            fs_doc: FSDocument = cls.get_subcollection_docs_pydantic_cls().parse_fs_doc_snapshot(doc_snap, read_subcollections=read_subcollections)
            doc_dict[doc_snap.id] = fs_doc
        return cls.parse_obj(doc_dict)

    @classmethod
    def get_subcollection_col_name(cls) -> str:
        # ABSTRACT method must be defined by childs
        # col name of sub collection e.g. plot_data
        raise NotImplementedError

    @classmethod
    def get_subcollection_docs_pydantic_cls(cls) -> Type[FSDocument]:
        # ABSTRACT method must be defined by childs
        raise NotImplementedError
        # returns class FSDocument for typing Optional[Dict[str, FSDocument]]
        # return cls.__dict__["__fields__"]["doc_dict"].sub_fields[0].outer_type_

    def update_doc_dict(self, subcollection_doc: FSDocument):
        self[subcollection_doc.doc_id] = subcollection_doc


