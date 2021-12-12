from datetime import datetime
from google.cloud import firestore
from mwfunctions.pydantic import OrderByDirection
from pydantic import BaseModel, Field
from typing import List, Optional, Union, Iterable
from contextlib import suppress

import logging
import google
import re

from mwfunctions.time import date_to_integer
from mwfunctions.exceptions import log_suppress
from mwfunctions import environment
from mwfunctions.pydantic.firestore.indexes import FSIndex, FSIndexItem
from mwfunctions.pydantic.base_classes import EnumBase

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)


##########################################################################
# Functions

CLIENT_CACHE = None

def create_client(project=None, use_cache=True):
    """ abstract firestore away """
    global CLIENT_CACHE
    if CLIENT_CACHE is None or not use_cache:
        project_id = project if project else environment.get_gcp_project()
        CLIENT_CACHE = firestore.Client(project=project_id)
    return CLIENT_CACHE

# class CachedFirestoreClient(object):
#     client = None
#     def __init__(self, project=None) -> None:
#         if self.client is None:
#             self.client = create_client(project)
        

# 0.028623104095458984s from bab in der firma
def get_document_snapshot_dict(path=None, collection_path=None, document_id=None, client=None,
                               transaction=None) -> dict:
    """ Provide path EXOR ( collection_name and document_id ) """
    return get_document_snapshot(path, collection_path, document_id, client, transaction=transaction).to_dict()


def split_correct_path(path):
    # No "/" at start for the python api
    path = path[1:] if path[0] == "/" else path
    collection_path, document_id = "/".join(path.split("/")[:-1]), "/".join(path.split("/")[-1:])
    return collection_path, document_id


def write_document_dict(data_dict, path=None, collection_path=None, document_id=None, overwrite_doc=False, client=None,
                        add_timestamp=False, array_union=False):
    """ Creates needed paths. Updates if not overwrite_doc.

    Args:
        data_dict (dict): Dict of properties which are either set or updated to firestore document
        path (str): Path as seen in web interface
        collection_path (str): firestore path to collection. E.g. col/doc/col/.../col
        doc_id (str): document id which is part of collection in collection_path
        overwrite_doc (bool, optional): Whether to set a document (overwrites existent) or update it. Defaults to False.
        client ([type], optional): Firestore client object. Defaults to None.
        add_timestamp (bool, optional): Whether to set timestamp property or not. Defaults to False.
        array_union (bool, optional): Whether to update existent arrays or overwrite it if do_set is False. Defaults to False.
    """
    assert path != (collection_path and document_id), "Provide path or collection_name and document_id"
    if path:
        collection_path, document_id = split_correct_path(path)
    client = client if client else create_client()
    if add_timestamp: data_dict.update({'timestamp': datetime.now()})

    # get reference of document which can execute update or set command on document
    doc_ref = client.collection(collection_path).document(str(document_id))

    # Set if overwrite_doc or it does not exist
    if overwrite_doc or not doc_ref.get().exists:
        doc_ref.set(data_dict)
    else:
        # if array_union is True, already existent array should be appended with new data but not overwritten
        # https://cloud.google.com/firestore/docs/manage-data/add-data#update_elements_in_an_array
        if array_union:
            for key, value in data_dict.items():
                if type(value) == list:
                    data_dict.update({key: firestore.ArrayUnion(value)})
        doc_ref.update(data_dict)

def delete_document(path):
    doc_ref = get_document_ref(path)
    doc_ref.delete()

def get_document_ref(path=None, collection_path=None, document_id=None, client=None):
    """ Reference of Firstore document

    :param path: Path to document like col/doc/col/doc/.../doc , defaults to None
    :type path: str, optional
    :param collection_path: Path to collection like col or col/doc/col/.../col, defaults to None
    :type collection_path: str, optional
    :param document_id: Firestore ID of document, defaults to None
    :type document_id: str, optional
    :param client: Firestore client, defaults to None
    :type client: Firestore client, optional
    :return: DocumentReference object of Firestore
    :rtype: DocumentReference
    """
    assert path != (collection_path and document_id), "Provide path or collection_name and document_id"
    if path:
        collection_path, document_id = split_correct_path(path)
    client = client if client else create_client()
    return client.collection(collection_path).document(document_id)

class FSComparisonOperator(str, EnumBase):
    GREATER_THAN=">"
    GREATER_OR_EQUAL=">="
    LESS_THAN="<"
    LESS_OR_EQUAL="<="
    EQUAL="=="
    NOT_EQUAL="!="
    ARRAY_CONTAINS="array_contains"
    ARRAY_CONTAINS_ANY="array_contains_any"
    IN="in"
    NOT_IN="not_in"
    # not possible in FS
    ARRAY_CONTAINS_ALL="array_contains_all"
    ARRAY_CONTAINS_NOT_ANY="array_contains_not_any"
    TEXT_CONTAINS_NOT_ANY="text_contains_not_any"
    TEXT_CONTAINS_NOT_ANY_BETWEEN_WORDS="text_contains_not_any_between_words"

class FSSimpleFilterQuery(BaseModel):
    field: str = Field(description="FS field to filter on", example="active")
    comparison_operator: FSComparisonOperator = Field(description="Query operator, e.g. '>', '==', '<'. https://firebase.google.com/docs/firestore/query-data/queries#query_operators", example="==")
    value: Union[bool,float,int,datetime,list,str] = Field(description="Value which should match the condition of operator. E.g. True. Value should have right type.", example=True)

    # make pydantic class hashable and drop dupplicates by set(List[FSSimpleFilterQuery]) possible
    def __eq__(self, other):
        return self.field==other.field and self.comparison_operator==other.comparison_operator and self.value==other.value

    def __hash__(self):  # make hashable BaseModel subclass
        return hash((type(self),) + tuple([x if type(x) != list else str(x) for x in self.__dict__.values()]))

def filter_simple_query_filters(simple_query_filters: List[FSSimpleFilterQuery], field: Optional[str]=None, comparison_operator: Optional[FSComparisonOperator]=None, value: Optional[Union[bool,float,int,datetime,list,str]] = None, use_equal_compare=True) -> List[FSSimpleFilterQuery]:
    filtered_simple_query_filters = simple_query_filters
    if field:
        # catching case of dict filter like keywords_stem.`vatertag`
        if use_equal_compare:
            filtered_simple_query_filters = list(filter(lambda x: x.field == field if not ".`" in x.field else x.field.split(".`")[0] == field, filtered_simple_query_filters))
        else:
            filtered_simple_query_filters = list(filter(lambda x: x.field != field if not ".`" in x.field else x.field.split(".`")[0] != field, filtered_simple_query_filters))

    if comparison_operator:
        if use_equal_compare:
            filtered_simple_query_filters = list(filter(lambda x: x.comparison_operator == comparison_operator, filtered_simple_query_filters))
        else:
            filtered_simple_query_filters = list(filter(lambda x: x.comparison_operator != comparison_operator, filtered_simple_query_filters))

    if value:
        if use_equal_compare:
            filtered_simple_query_filters = list(filter(lambda x: x.value == value, filtered_simple_query_filters))
        else:
            filtered_simple_query_filters = list(filter(lambda x: x.value != value, filtered_simple_query_filters))

    return filtered_simple_query_filters

def filter_by_fs_comparison_operator(field_value: Union[bool,float,int,datetime,list,str], comparison_operator:FSComparisonOperator, compare_value: Union[bool,float,int,datetime,list,str], case_sensitive=False):
    """ General filter function for all types of FSComparisonOperator.
        Can be used to filter Data after FS documents are streamed. Helpfull if to many indexes would be required.
        Return bool if field_value matches compare_value by comparison_operator
    """
    # make operations possible for datetime objects
    if isinstance(field_value, datetime):
        field_value = date_to_integer(field_value)
    if isinstance(compare_value, datetime):
        compare_value = date_to_integer(compare_value)
    # try to transform type to float if not both are string
    # try to transform type to float if one is number and the other one is string
    with suppress(ValueError):
        if isinstance(field_value, str) and (isinstance(compare_value, int) or isinstance(compare_value, float)): field_value = float(field_value)
    with suppress(ValueError):
        if isinstance(compare_value, str) and (isinstance(field_value, int) or isinstance(field_value, float)): compare_value = float(compare_value)

    if comparison_operator == FSComparisonOperator.GREATER_THAN:
        return field_value > compare_value
    elif comparison_operator == FSComparisonOperator.GREATER_OR_EQUAL:
        return field_value >= compare_value
    elif comparison_operator == FSComparisonOperator.LESS_THAN:
        return field_value < compare_value
    elif comparison_operator == FSComparisonOperator.LESS_OR_EQUAL:
        return field_value <= compare_value
    elif comparison_operator == FSComparisonOperator.EQUAL:
        return field_value == compare_value
    elif comparison_operator == FSComparisonOperator.NOT_EQUAL:
        return field_value != compare_value
    elif comparison_operator == FSComparisonOperator.ARRAY_CONTAINS:
        assert isinstance(field_value, list), f"field_value {field_value} must be of type list but is {type(field_value)}"
        return compare_value in field_value
    elif comparison_operator in [FSComparisonOperator.ARRAY_CONTAINS_ANY, FSComparisonOperator.ARRAY_CONTAINS_NOT_ANY]:
        assert isinstance(field_value, list), f"field_value {field_value} must be of type list but is {type(field_value)}"
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        contains_any = any(compare_value_i in field_value for compare_value_i in compare_value)
        return contains_any if comparison_operator == FSComparisonOperator.ARRAY_CONTAINS_ANY else not contains_any
    elif comparison_operator == FSComparisonOperator.ARRAY_CONTAINS_ALL:
        assert isinstance(field_value, list), f"field_value {field_value} must be of type list but is {type(field_value)}"
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        return all(compare_value_i in field_value for compare_value_i in compare_value)
    elif comparison_operator == FSComparisonOperator.IN:
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        return field_value in compare_value
    elif comparison_operator == FSComparisonOperator.NOT_IN:
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        return field_value not in compare_value
    elif comparison_operator in [FSComparisonOperator.TEXT_CONTAINS_NOT_ANY, FSComparisonOperator.TEXT_CONTAINS_NOT_ANY_BETWEEN_WORDS]:
        assert isinstance(field_value, str), f"field_value {field_value} must be of type str but is {type(field_value)}"
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        whitespace = " " if comparison_operator == FSComparisonOperator.TEXT_CONTAINS_NOT_ANY_BETWEEN_WORDS else ""
        return not any([bool(re.search(whitespace + v + whitespace, field_value, re.IGNORECASE if not case_sensitive else 0)) for v in compare_value])
    else:
        raise NotImplementedError

def get_collection_ref(collection_path, client: google.cloud.firestore_v1.client.Client=None) -> google.cloud.firestore_v1.collection.CollectionReference:
    # No "/" at start for the python api
    collection_path = collection_path[1:] if collection_path[0] == "/" else collection_path
    assert (len(collection_path.split(
        "/")) % 2) != 0, "path needs be a collection path with odd number of '/' seperated elements"
    client = client if client else create_client()
    return client.collection(collection_path)


def get_collection_query(collection_path, simple_query_filters: Optional[List[FSSimpleFilterQuery]] = None, client: google.cloud.firestore_v1.client.Client = None) -> Union[google.cloud.firestore_v1.collection.CollectionReference, google.cloud.firestore_v1.query.Query]:
    """ Returns a collection reference (query) with optional query_filters (e.g. where or in or </> conditions)

    Args:
        collection_path (str): Collection Firestore path
        simple_query_filters: List of FSSimpleFilterQuery which will be queried in order.

    Returns:
        collection reference object

    """
    simple_query_filters = simple_query_filters or []
    query = get_collection_ref(collection_path, client=client)

    # if simple_query_filters are provided multiple where conditions are appended to firestore query
    for simple_query_filter in simple_query_filters:
        query = query.where(simple_query_filter.field, simple_query_filter.comparison_operator, simple_query_filter.value)
    return query

def get_docs_snap_iterator(collection_path: str, simple_query_filters: Optional[List[FSSimpleFilterQuery]] = None, client: google.cloud.firestore_v1.client.Client = None) -> Iterable:
    """ Snapshot (downloaded, not lazy). Filters applied inorder."""
    return get_collection_query(collection_path, simple_query_filters=simple_query_filters, client=client).stream()

def get_docs_ref_iterator(collection_path, client: google.cloud.firestore_v1.client.Client = None):
    """Lazy load, need to call .get"""
    return get_collection_ref(collection_path, client=client).list_documents()


def get_docs_batch(collection_path, limit: Optional[int]=None, order_by: Optional[str]=None, start_after=None, direction: OrderByDirection = OrderByDirection.ASC, simple_query_filters: Optional[List[FSSimpleFilterQuery]] = None, start_after_doc_id: Optional[str]=None, client=None):
    """Return a generator object of one batch(size like limit) of documents
        Query can be filtered, sorted or start after specific values of order_by field.

    Args:
        collection_path (str): Collection Firestore path
        limit (int): Size of batch
        order_by (str, optional): Firestore property of document which should be used for ordering. Important for start_after to work properly. Defaults to None.
        start_after (str, optional): Value of Firestore property defined in order_by. One document after this value new batch is started. Defaults to None.
        direction (str, optional): Whether to order in ascending or descending order. Defaults to "asc".
        simple_query_filters: List of FSSimpleFilterQuery which will be queried in order.
        client (google.cloud.firestore_v1.client.Client): Google cloud Firestore client.Defaults to None".

    Returns:
        iterable
    """
    # TODO: Test if this works like before

    # No "/" at start for the python api
    collection_path = collection_path[1:] if collection_path[0] == "/" else collection_path
    assert (len(collection_path.split(
        "/")) % 2) != 0, "path needs be a collection path with odd number of '/' seperated elements"
    if start_after:
        assert order_by, "if start_after then order_by needs to be set"
    if order_by:
        assert direction in ["asc", "desc"], 'direction elem ["asc", "desc"]'
        direction = firestore.Query.ASCENDING if direction == OrderByDirection.ASC else firestore.Query.DESCENDING

    query = get_collection_query(collection_path, simple_query_filters=simple_query_filters, client=client)
    if order_by:
        query = query.order_by(order_by, direction=direction)
    if start_after:
        query = query.start_after({
            order_by: start_after
        })

    comparison_operators_preventing_start_after_doc_id = [FSComparisonOperator.IN, FSComparisonOperator.NOT_IN]
    is_start_after_doc_id_possible = True if not simple_query_filters else not any([len(filter_simple_query_filters(simple_query_filters, comparison_operator=comparison_operator)) > 0 for comparison_operator in comparison_operators_preventing_start_after_doc_id])
    # TODO: Does this query works if simple_query_filters contain "IN" statement
    if start_after_doc_id and is_start_after_doc_id_possible:
        doc_snap = get_document_snapshot(f"{collection_path}/{start_after_doc_id}")
        query = query.start_after(doc_snap)
    if not is_start_after_doc_id_possible: print(f"Warning: start_after_doc_id {start_after_doc_id} cannot be used since simple_query_filter exists with IN operation")

    if limit:
        query = query.limit(limit)

    return query.stream()


def get_document_snapshot(path=None, collection_name=None, document_id=None, client=None, transaction=None):
    """ Provide path EXOR ( collection_name and document_id )
    snapshot.to_dict() for data, snapshot.reference for the document reference id
    """
    return get_document_ref(path, collection_name, document_id, client).get(transaction=transaction)

def does_document_exists(path=None, collection_name=None, document_id=None, client=None, transaction=None) -> bool:
    return get_document_snapshot(path=path, collection_name=collection_name, document_id=document_id, client=client, transaction=transaction).exists

def validate_document_name(document_name):
    """ validates if document_name is suitable for firestore document name """
    if type(document_name) != str:
        raise ValueError("document_name is not a string")
    else:
        if "." in document_name or ".." in document_name or "/" in document_name:
            raise ValueError("document_name does not fit the firestore naming rules")


def get_field(doc_path, field):
    doc_dict = get_document_snapshot_dict(path=doc_path)
    return doc_dict[field]

def set_field(doc_path, field, field_value):
    write_document_dict({field: field_value},path=doc_path, overwrite_doc=False)

def del_field(doc_path, field):
    with log_suppress(f"Could not find {doc_path} : {field}", google.api_core.exceptions.NotFound):
        doc_ref = get_document_ref(doc_path)
        doc_ref.update({field: firestore.DELETE_FIELD})


def increment_field(doc_path, field, amount):
    doc_ref = get_document_ref(doc_path)
    doc_ref.update({field: firestore.Increment(amount)})


@firestore.transactional
def atomic_add(transaction, doc_path, field, amount, client):
    """ Its only allowed to write once per second... """
    doc_dict = get_document_snapshot_dict(path=doc_path, client=client, transaction=transaction)
    doc_dict[field] += amount
    write_document_dict(doc_dict, path=doc_path, client=client)
    return doc_dict[field]


# custom MW

def do_index_items_contain_simple_query_filter(simple_query_filters: FSSimpleFilterQuery, fs_index_items: List[FSIndexItem]) -> bool:
    # TODO make True only if index_item.index_option matches simple_query_filters.comparison_operator
    return any([index_item.field == simple_query_filters.field for index_item in fs_index_items])

def filter_simple_query_filters_by_fs_indexes(simple_query_filters: Optional[List[FSSimpleFilterQuery]], fs_collection_indexes: List[FSIndex], order_by, order_by_direction: OrderByDirection, is_keyword_search=False) -> Optional[List[FSSimpleFilterQuery]]:
    # return a list of FSSimpleFilterQuery which are possible to query by fiven list of FSIndex
    if simple_query_filters==None: return None
    # all query filters that can be combined without index
    simple_query_filters_possible_without_index = [FSComparisonOperator.ARRAY_CONTAINS, FSComparisonOperator.EQUAL, FSComparisonOperator.NOT_EQUAL,
     FSComparisonOperator.IN, FSComparisonOperator.NOT_IN, FSComparisonOperator.ARRAY_CONTAINS_ANY]
    if is_keyword_search:
        return list(filter(lambda simple_query_filter: simple_query_filter.comparison_operator in simple_query_filters_possible_without_index, simple_query_filters))

    # init filtered_simple_query_filters with all order by filters
    filtered_simple_query_filters: List[FSSimpleFilterQuery] = list(filter(lambda x: x.field == order_by, simple_query_filters))
    # get those indexes which are available for order by (order by field must be last index)
    available_fs_collection_indexes: List[FSIndex] = list(filter(lambda index: index.index_items[-1].field == order_by and index.index_items[-1].index_option == order_by_direction, fs_collection_indexes))
    # does only work for max. 3 field indexes
    if any([len(x.index_items) > 3 for x in available_fs_collection_indexes]): print("Warning: Index exists with more than 3 fields. Algo cannot find matching simple query filter at the moment")
    # append all not order by simple query filters to filtered_simple_query_filters
    for simple_query_filter_i in simple_query_filters:
        for simple_query_filter_j in simple_query_filters:
            if simple_query_filter_i == simple_query_filter_j: continue # Only different filters are checked in inner loop
            # check if both simple_query_filters i and j are possible with available_fs_collection_indexes
            # last index_item can be excluded since available_fs_collection_indexes only contains indexes where order_by is field of last index item
            # Condition:  Both simple_query_filter_i and simple_query_filter_j must be part of fs_collection_index.index_items[0:-1] (order by field is excluded)
            #             If any index of available_fs_collection_indexes matches condition we cann add both filters to filtered_simple_query_filters
            if any([all([do_index_items_contain_simple_query_filter(simple_query_filter_i, fs_collection_index.index_items[0:-1]),
                         do_index_items_contain_simple_query_filter(simple_query_filter_j, fs_collection_index.index_items[0:-1])]
                        )
                    for fs_collection_index in available_fs_collection_indexes]):
                filtered_simple_query_filters.extend([simple_query_filter_i, simple_query_filter_j])

        # check if single simple_query_filters i is possible with available_fs_collection_indexes
        if any([do_index_items_contain_simple_query_filter(simple_query_filter_i, fs_collection_index.index_items[0:1])
                for fs_collection_index in available_fs_collection_indexes if len(fs_collection_index.index_items) == 2]):
            filtered_simple_query_filters.append(simple_query_filter_i)

    # drop duplicates
    return list(set(filtered_simple_query_filters))


