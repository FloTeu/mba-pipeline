import copy
from datetime import datetime
from google.cloud import firestore
from mwfunctions.pydantic import OrderByDirection
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Union, Dict, Iterable
import logging
import google
import uuid

from mwfunctions.time import date_to_integer
from mwfunctions.exceptions import log_suppress
from mwfunctions import environment
from mwfunctions.pydantic.firestore.firestore_classes import FSDocument, FSMBAShirtOrderBy, MBA_SHIRT_ORDERBY_DICT, get_bsr_range_list, MBAShirtOrderByField, bsr2bsr_range_value
from mwfunctions.pydantic.firestore.indexes import MBA_SHIRTS_COLLECTION_INDEXES, FSIndex, FSIndexItem, FSMbaShirtsIndexField
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
    ARRAY_CONTAINS_ALL="array_contains_all"
    IN="in"
    NOT_IN="not_in"

class FSSimpleFilterQuery(BaseModel):
    field: str = Field(description="FS field to filter on", example="active")
    comparison_operator: FSComparisonOperator = Field(description="Query operator, e.g. '>', '==', '<'. https://firebase.google.com/docs/firestore/query-data/queries#query_operators", example="==")
    value: Union[bool,float,int,datetime,list,str] = Field(description="Value which should match the condition of operator. E.g. True. Value should have right type.", example=True)

    # make pydantic class hashable and drop dupplicates by set(List[FSSimpleFilterQuery]) possible
    def __eq__(self, other):
        return self.field==other.field and self.comparison_operator==other.comparison_operator and self.value==other.value

    def __hash__(self):  # make hashable BaseModel subclass
        return hash((type(self),) + tuple([x if type(x) != list else str(x) for x in self.__dict__.values()]))

def filter_by_fs_comparison_operator(field_value: Union[bool,float,int,datetime,list,str], comparison_operator:FSComparisonOperator, compare_value: Union[bool,float,int,datetime,list,str]):
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
    if isinstance(field_value, str) and not isinstance(compare_value, str): field_value = float(field_value)
    if isinstance(compare_value, str) and not isinstance(field_value, str): compare_value = float(compare_value)

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
    elif comparison_operator == FSComparisonOperator.ARRAY_CONTAINS_ANY:
        assert isinstance(field_value, list), f"field_value {field_value} must be of type list but is {type(field_value)}"
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        return any(compare_value_i in field_value for compare_value_i in compare_value)
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


def get_docs_batch(collection_path, limit: Optional[int]=None, order_by: Optional[str]=None, start_after=None, direction: OrderByDirection = OrderByDirection.ASC, simple_query_filters: Optional[List[FSSimpleFilterQuery]] = None, client=None):
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

def filter_simple_query_filters_by_fs_indexes(simple_query_filters: Optional[List[FSSimpleFilterQuery]], fs_collection_indexes: List[FSIndex], order_by, order_by_direction: OrderByDirection) -> Optional[List[FSSimpleFilterQuery]]:
    # return a list of FSSimpleFilterQuery which are possible to query by fiven list of FSIndex
    if simple_query_filters==None: return None
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

class OrderByCursor(object):
    def __init__(self, cursor, order_by_key, keywords_stem_list: Optional[list]=None, filter_takedown_value: Optional[bool]=None):
        self.cursor: Union[str,int,datetime,float] = cursor # can be of any type and depends on order by field in FS
        self.keywords_stem_list = keywords_stem_list # list of stemmed keywords which where used to get data from FS
        self.filter_takedown_value = filter_takedown_value
        self.order_by_key = order_by_key

    @staticmethod
    def get_order_by_key(order_by, order_by_direction, search_key_stem_list: Optional[list]=None, filter_takedown: Optional[bool]=None):
        # filter_takedown is true if only takedown data should be returned
        if order_by and order_by_direction:
            order_by_key = f"{order_by}_{order_by_direction}"
            search_key_append = f"_{'_'.join(search_key_stem_list)}" if search_key_stem_list else ""
            filter_takedown_append = f"_{filter_takedown}" if filter_takedown!=None else ""
            return f"{order_by_key}{search_key_append}{filter_takedown_append}"
        else:
            return None

    @staticmethod
    def get_keywords_not_queried_already(order_by_cursors, keyword_list: Optional[list]=None, filter_takedown_value: Optional[bool]=None) -> List[str]:
        if not keyword_list: return []
        keywords_not_queried_already = []
        for keyword in keyword_list:
            if not any([keyword in x.keywords_stem_list for x in order_by_cursors if x.filter_takedown_value == filter_takedown_value]):
                keywords_not_queried_already.append(keyword)
        return keywords_not_queried_already

    @staticmethod
    def get_order_by_key_by_keyword_list(order_by_cursors, keyword_list: Optional[list]=None)-> Optional[str]:
        if not keyword_list: return None
        # TODO return order_by_key where most keywords_stem_list matches keyword_list
        # simple algo returning first match
        for order_by_cursor_obj in order_by_cursors:
            if any([x in keyword_list for x in order_by_cursor_obj.keywords_stem_list]):
                return order_by_cursor_obj.order_by_key
        return None


class CacherDocument(object):
    def __init__(self, fs_document, doc_id, order_by_key=None, order_by=None, order_by_direction: OrderByDirection =None):
        order_by_key = order_by_key if order_by_key else f"{order_by}_{order_by_direction}" if (order_by and order_by_direction) else None
        self.order_by_key_list: list=[order_by_key] if order_by_key else []
        self.fs_document: FSDocument = fs_document
        self.doc_id = doc_id

    def add_order_by_key(self, order_by_key=None, order_by=None, filter_takedown=None, order_by_direction: OrderByDirection =None):
        assert order_by_key or (order_by and order_by_direction), "Either order_by_key or (order_by and order_by_direction) must be set"
        order_by_key = order_by_key if order_by_key else OrderByCursor.get_order_by_key(order_by, order_by_direction, filter_takedown=filter_takedown)
        if order_by_key not in self.order_by_key_list:
            self.order_by_key_list.append(order_by_key)

    def is_part_of_order_by(self, order_by_key):
        return order_by_key in self.order_by_key_list


class FsDocumentsCacher(object):
    """ This class can be continiously filled with FS documents. Each cacher should only be used for one collection.
        Procedure:
            1. Cacher is initialised by collection path and pydantic class
            2. Cacher request gets order by and filters.
            3. All documents are stored in one dict independently of order by
            4. Cacher stores at which point he loaded new data for order by
            5. If filter does not match filters after _max_load_new_batch_nr for order by all documents should be used
    """

    def __init__(self, pydantic_class: FSDocument, collection_path: str, client=None):
        """
            order_by: if defined cacher gets new batch in direction of order_by
        """
        # TODO add cursor for different order bys
        self._order_by_cursors: Dict[str, OrderByCursor] = {} # cursor is updated each time cacher is filled with new data and defines last element got from FS. Key is a order by key defined by get_order_by_key
        self._max_load_new_batch_nr = 5 # maximum number of loops to prevent endless fetch of new data
        self._uuid2filter_true_counter = {}
        self._uuid2doc_id_found = {} # Whether cursor for doc id was found or not
        self._uuid2statistics: Dict[str, dict] = {}
        self._uuid2order_by_cursor: Dict[str, Dict[str, Union[float, int, str, datetime]]] = {} # {tmp_id: {order_by_str: order_by_cursor}}

        self.client = client if client else create_client()
        self.pydantic_class: FSDocument = pydantic_class
        self.collection_path = collection_path
        self.doc_id2cache_doc: Dict[str, CacherDocument] = {}

    def update_order_by_cursors(self, order_by_key, order_by, doc_pydantic, keywords_stem_list: Optional[list]=None, filter_takedown_value: Optional[bool]=None):
        if order_by_key not in self._order_by_cursors:
            if doc_pydantic and order_by:
                self._order_by_cursors[order_by_key] = OrderByCursor(doc_pydantic[order_by], order_by_key, keywords_stem_list=keywords_stem_list, filter_takedown_value=filter_takedown_value)
        else:
            if doc_pydantic and order_by:
                self._order_by_cursors[order_by_key].cursor = doc_pydantic[order_by]

    def is_simple_fs_order_by_query(self, simple_query_filters: Optional[List[FSSimpleFilterQuery]], order_by, order_by_direction, filter_takedown):
        """ Whether query task is a simple order by query (i,e, order by query from starting point without further filerings except takedown):
            Rules:
                * order by is included
                * Other filters are only takedown filter
                + order_by cursor matches order_by_filter start

            considered are only get_fs_possible_filters

            Not simple_order_by_queries are for example:
                * Search key query
        """
        # TODO: order_by None could also be a simple order_by_query but alphabetically (i.e. FS infrsatructure sorts docs)
        if order_by==None: return False
        if simple_query_filters==None and order_by!=None: return True
        order_by_key = OrderByCursor.get_order_by_key(order_by, order_by_direction, filter_takedown=filter_takedown)
        fs_possible_filters: Optional[List[FSSimpleFilterQuery]] = FsDocumentsCacher.get_fs_possible_filters(simple_query_filters, MBA_SHIRTS_COLLECTION_INDEXES, order_by, order_by_direction)
        numer_of_filters_not_takedown_or_order_by = len(list(filter(lambda x: x.field!=order_by and x.field!="takedown", fs_possible_filters)))
        if len(fs_possible_filters) == 0: return True
        # CHeck if contains only order_by start or cursor or takedown_filter (exclude filters from other direction e.g. high bsr_max value)
        def does_comparison_operator_matches_direction(comparison_operator: FSComparisonOperator, order_by_direction: OrderByDirection):
            if comparison_operator in [FSComparisonOperator.GREATER_THAN, FSComparisonOperator.GREATER_OR_EQUAL] and order_by_direction == OrderByDirection.ASC:
                return True
            elif comparison_operator in [FSComparisonOperator.LESS_THAN, FSComparisonOperator.LESS_OR_EQUAL] and order_by_direction == OrderByDirection.DESC:
                return True
            else:
                return False
        order_by_filters_default_direction = list(filter(lambda x: x.field==order_by and does_comparison_operator_matches_direction(x.comparison_operator,  MBA_SHIRT_ORDERBY_DICT[order_by].direction), fs_possible_filters))
        # case: only order by filters and start from beginning
        if numer_of_filters_not_takedown_or_order_by == 0 and len(order_by_filters_default_direction) == 0: return True
        # TODO: check if order_by_filters[0].value == self._order_by_cursors[order_by_key].cursor works
        # no order by filter is considered like from start filter
        if len(order_by_filters_default_direction) == 0:
            is_start_or_cursor_order_by_filter = True
        else:
            is_start_value = order_by_filters_default_direction[0].value == MBA_SHIRT_ORDERBY_DICT[order_by].start_value
            is_cursor_value = order_by_filters_default_direction[0].value == self._order_by_cursors[order_by_key].cursor if order_by_key in self._order_by_cursors else False
            is_start_or_cursor_order_by_filter = is_start_value or is_cursor_value
        # takedown_filters = list(filter(lambda x: x.field=="takedown", fs_possible_filters))
        # case: only one order by filter form start or cursor
        if numer_of_filters_not_takedown_or_order_by == 0 and len(order_by_filters_default_direction) == 1 and is_start_or_cursor_order_by_filter:
            return True
        else:
            return False
        # # case: only takedown filter
        # if numer_of_filters_not_takedown_or_order_by == 0 and len(takedown_filters) == 1: return True
        # # case: two filters 1. order by filter from start or cursor 2. takedown filter
        # if numer_of_filters_not_takedown_or_order_by == 0 and is_start_or_cursor_order_by_filter and len(order_by_filters_default_direction) == 1 and len(takedown_filters) == 1: return True
        # return False

    def test_filters(self):
        # testing multiple filter operation for master_filter
        ts = time.time()
        docs = []
        for i in range(200):
            docs_iter = get_docs_batch(self.collection_path, limit=batch_size, order_by=order_by,
                                       direction=order_by_direction, client=self.client,
                                       start_after=order_by_cursor, simple_query_filters=[FSSimpleFilterQuery(field="bsr_last", comparison_operator=FSComparisonOperator.GREATER_OR_EQUAL, value=i*10000), FSSimpleFilterQuery(field="bsr_last", comparison_operator=FSComparisonOperator.LESS_OR_EQUAL, value=i*10000+100)])
            for doc in docs_iter:
                docs.append(doc)
        print("Elapsed time %.2fs"%(time.time() - ts), len(docs))

    def get_order_by_cursor_wrapper(self, order_by_key, tmp_id, order_by, order_by_direction: OrderByDirection, is_simple_fs_order_by_query, doc_id_cursor=None):
        """ Function to get order by cursor
            Process:
                1. If is_simple_fs_order_by_query (no filters, that prevent normal order by cursor logic) -> get last cursor in _order_by_cursors
                2. Elif cursor found in tmp_id (id for one API get_batch request) -> take this one
                3. Elif Try to get cursor with doc_id_cursor with all docs already stored in self.doc_id2cache_doc
        """
        order_by_cursor = None
        if is_simple_fs_order_by_query:
            order_by_cursor = self._order_by_cursors[order_by_key].cursor if order_by_key and order_by_key in self._order_by_cursors else None
        elif not order_by_cursor and tmp_id in self._uuid2order_by_cursor and order_by in self._uuid2order_by_cursor[tmp_id]:
            order_by_cursor = self._uuid2order_by_cursor[tmp_id][order_by]
        elif not order_by_cursor:
            order_by_cursor = self.get_order_by_cursor(doc_id_cursor, order_by, reverse=order_by_direction == OrderByDirection.DESC)
        return order_by_cursor

    def load_batch_in_cache(self, batch_size=100, order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, doc_id_cursor=None, tmp_id=None):
        """ Loads new batch of FS documents in cache.
            Be carefull with simple_query_filter, since indexes might be needed. order by filtering is always possible
        """

        # order by key can be for example bsr_last_asc or "bsr_last_Vatertag Angler"
        search_key_stem_list: Optional[list] = self.get_value_by_filters(simple_query_filters, "keywords_stem_list")
        filter_takedown_value: Optional[bool] = self.get_value_by_filters(simple_query_filters, "takedown")
        # TODO: if keyword was already queried -> prevent quering same data again
        keywords_not_queried_already = OrderByCursor.get_keywords_not_queried_already(list(self._order_by_cursors.values()), search_key_stem_list, filter_takedown_value)
        # if keywords stem exists but every keyword was queried already -> use existent order_by_key
        if search_key_stem_list and (keywords_not_queried_already == None or len(keywords_not_queried_already) == 0):
            order_by_key = OrderByCursor.get_order_by_key_by_keyword_list(list(self._order_by_cursors.values()), search_key_stem_list)
        else:
            order_by_key = OrderByCursor.get_order_by_key(order_by, order_by_direction, search_key_stem_list=search_key_stem_list, filter_takedown=filter_takedown_value)
        # should be true for normal trend page, bsr page etc.
        is_simple_fs_order_by_query = self.is_simple_fs_order_by_query(simple_query_filters, order_by, order_by_direction, filter_takedown=filter_takedown_value)

        order_by_cursor = self.get_order_by_cursor_wrapper(order_by_key, tmp_id, order_by, order_by_direction, is_simple_fs_order_by_query, doc_id_cursor=doc_id_cursor)

        # TODO: use collection call without read to get all doc ids without read doc data (cheaper
        docs_iter = get_docs_batch(self.collection_path, limit=batch_size, order_by=order_by, direction=order_by_direction, client=self.client, start_after=order_by_cursor, simple_query_filters=simple_query_filters)
        doc_pydantic = None
        for doc in docs_iter:
            try:
                if tmp_id:
                    self._uuid2statistics[tmp_id]["nr_fs_reads"] = self._uuid2statistics[tmp_id]["nr_fs_reads"] + 1 if "nr_fs_reads" in self._uuid2statistics[tmp_id] else 0
                # Transform doc to pydantic
                doc_pydantic: FSDocument = self.pydantic_class.parse_fs_doc_snapshot(doc)
                # only update order by data if query was not filtered
                # TODO: Does algo work like intendend if some docuemnts to not contain order by statement?
                order_by_key_update = order_by_key if is_simple_fs_order_by_query or search_key_stem_list else None
                # Add to CacherDocument if not exists or add order by data to document
                if doc.id not in self.doc_id2cache_doc:
                    self.doc_id2cache_doc[doc.id] = CacherDocument(doc_pydantic, doc.id, order_by_key=order_by_key_update)
                else:
                    if is_simple_fs_order_by_query or search_key_stem_list:
                        self.doc_id2cache_doc[doc.id].add_order_by_key(order_by_key=order_by_key_update)

            except ValidationError as e:
                print(f"Could not parse document with id {doc.id} to pydantic class", str(e))

        # update order by cursor
        if order_by:
            if tmp_id not in self._uuid2order_by_cursor: self._uuid2order_by_cursor[tmp_id] = {}
            self._uuid2order_by_cursor[tmp_id][order_by] = doc_pydantic[order_by] if doc_pydantic else None
        # Update cursor only if data was loaded without none order by filtering
        if is_simple_fs_order_by_query or search_key_stem_list:
            self.update_order_by_cursors(order_by_key, order_by, doc_pydantic, keywords_stem_list=search_key_stem_list, filter_takedown_value=filter_takedown_value)

    @staticmethod
    def filter_fs_document_by_simple_query_filter(fs_document: FSDocument, simple_query_filter: FSSimpleFilterQuery) -> bool:
        fs_field_value = fs_document[simple_query_filter.field]
        return filter_by_fs_comparison_operator(fs_field_value, simple_query_filter.comparison_operator, simple_query_filter.value)

    def filter_fs_document_by_simple_query_filters(self, cacher_doc: CacherDocument, tmp_id,
                                                   simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, batch_size=None, doc_id_cursor: Optional[str]=None,
                                                   order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, filter_docs_by_order_by_key:bool=True, filter_takedown: Optional[bool] = None) -> bool:
        # if doc_id_cursor is provided, skip until cursor was found
        if doc_id_cursor!=None and cacher_doc.fs_document.doc_id == doc_id_cursor:
            self._uuid2doc_id_found[tmp_id] = True
            return False # return false to skip cursor doc id element (already shown in frontend)
        if doc_id_cursor!=None and not self._uuid2doc_id_found[tmp_id]: return False

        # if enough cached docs are found (i.e. more than batch_size requires) we dont have to check ither docs and return False
        # If not filter_docs_by_order_by_key, all docs should be filtered to sort afterwards by order by and get right docs
        if filter_docs_by_order_by_key and batch_size and self._uuid2filter_true_counter[tmp_id] > batch_size: return False

        # filter only those which are connected to order_by_key
        # Normal case: simple order by query without filtering -> no problem (only cacher_doc.is_part_of_order_by() are used)
        # Special case: order by query with multiple filters -> after n batch loads desired output size ist not reached because of filters
        #               -> Solution: ignore (only cacher_doc.is_part_of_order_by() are used)
        if filter_docs_by_order_by_key and order_by and order_by_direction:
            if not cacher_doc.is_part_of_order_by(OrderByCursor.get_order_by_key(order_by, order_by_direction, filter_takedown=filter_takedown)): return False
        if simple_query_filters == None or simple_query_filters == []:
            self._uuid2filter_true_counter[tmp_id] += 1
            return True
        filter_bool = all([FsDocumentsCacher.filter_fs_document_by_simple_query_filter(cacher_doc.fs_document, simple_query_filter) for simple_query_filter in simple_query_filters])
        if filter_bool: self._uuid2filter_true_counter[tmp_id] += 1
        return filter_bool

    def filter_fs_doc_dict(self, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, batch_size=None,
                           order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, filter_docs_by_order_by_key=True, doc_id_cursor: Optional[str]=None) -> List[CacherDocument]:
        # filter is only refernece to filter_fs_document_by_simple_query_filters function
        filter_takedown_value: Optional[bool] = self.get_value_by_filters(simple_query_filters, "takedown")
        # TODO: if doc_id_cursor self.doc_id2cache_doc.values() must be sorted by order by. otherwise doc_id_cursor cuts valid documents
        tmp_id = uuid.uuid4().hex
        self._uuid2filter_true_counter[tmp_id] = 0
        self._uuid2doc_id_found[tmp_id] = False
        fs_document_filter = filter(lambda cacher_doc:
                                    self.filter_fs_document_by_simple_query_filters(cacher_doc, tmp_id, simple_query_filters, order_by=order_by, batch_size=batch_size, filter_takedown=filter_takedown_value,
                                    order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key, doc_id_cursor=doc_id_cursor), self.doc_id2cache_doc.values())
        cacher_docs: List[CacherDocument] = list(fs_document_filter)
        del self._uuid2filter_true_counter[tmp_id]
        del self._uuid2doc_id_found[tmp_id]
        return cacher_docs

    @staticmethod
    def get_fs_possible_filters(simple_query_filters: Optional[List[FSSimpleFilterQuery]], fs_collection_indexes: List[FSIndex], order_by, order_by_direction: OrderByDirection) -> Optional[List[FSSimpleFilterQuery]]:
        """ Returns a filtered list of query filters for FS which are possible to filter directly in FS query.
            order by filters are always possible. Further filters need an index file

            Hint: Last index must be the one for order by operation
            Required Indexes:
                * keywords_stem_list: ARRAY, takedown: ASC, bsr_last: ASC
                * bsr_last_range: ASC, takedown: ASC, trend_nr: ASC
                * bsr_last_range: ASC, takedown: ASC, upload_date: DESC
                * bsr_last_range: ASC, takedown: ASC, bsr_change: ASC
                * bsr_last_range: ASC, takedown: ASC, price_last: ASC
            Indexes available: MBA_SHIRTS_COLLECTION_INDEXES
        """
        fs_possible_filters = filter_simple_query_filters_by_fs_indexes(simple_query_filters, fs_collection_indexes, order_by, order_by_direction)
        if fs_possible_filters == None: return None
        # TODO: filter simple_query_filters by available fs_collection_indexes
        # is_keyword_search = any([x.field == "keywords_stem_list" for x in simple_query_filters])
        # fs_possible_filters = copy.deepcopy(list(filter(lambda x: x.field == order_by or x.field == "keywords_stem_list" or x.field == "takedown" or x.field == "bsr_last_range", simple_query_filters)))
        # change array contains all to array contains any, because FS cannot query ARRAY_CONTAINS_ALL operation
        for x in fs_possible_filters:
            if x.comparison_operator == FSComparisonOperator.ARRAY_CONTAINS_ALL:
                x.comparison_operator = FSComparisonOperator.ARRAY_CONTAINS_ANY
        return fs_possible_filters

    @staticmethod
    def get_value_by_filters(simple_query_filters: Optional[List[FSSimpleFilterQuery]], fs_fied_str:str):
        if simple_query_filters == None: return None
        filtered_filters= list(filter(lambda x: x.field == fs_fied_str, simple_query_filters))
        return filtered_filters[0].value if len(filtered_filters) > 0 else None

    # def load_data_with_order_by_upload_date(self, bsr_min, bsr_max, load_batch_in_cache_size, load_batch_id, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, doc_id_cursor=None):
    #     simple_query_filters = [] if simple_query_filters == None else simple_query_filters
    #     bsr_range_start = int(bsr_min / 100000) if type(bsr_min) == int else 0
    #     bsr_range_end = int(bsr_max / 100000) if type(bsr_max) == int else 50
    #     simple_query_filters.append(FSSimpleFilterQuery(field="bsr_last_range",
    #                                                     comparison_operator=FSComparisonOperator.IN,
    #                                                     value=get_bsr_range_list((bsr_range_start, bsr_range_end),
    #                                                                              min(bsr_range_end - bsr_range_start,
    #                                                                                  10))))
    #     # TODO: Dont send tmp_id because threat of using false order by cursor..
    #     self.load_batch_in_cache(batch_size=load_batch_in_cache_size, order_by=MBAShirtOrderByField.UPLOAD,
    #                              order_by_direction=OrderByDirection.DESC, simple_query_filters=simple_query_filters,
    #                              tmp_id=None, doc_id_cursor=doc_id_cursor)
    #
    # def load_data_with_order_by_upload_date_with_filters(self, load_batch_in_cache_size, get_batch_request_id, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, doc_id_cursor=None):
    #     if not simple_query_filters: return None
    #     # get bsr_min, bsr_max
    #     bsr_filters=list(filter(lambda x: x.field == MBAShirtOrderByField.BSR, simple_query_filters))
    #     bsr_min = None
    #     bsr_max = None
    #     for bsr_filter in bsr_filters:
    #         if bsr_filter.comparison_operator in [FSComparisonOperator.GREATER_OR_EQUAL, FSComparisonOperator.GREATER_THAN]:
    #             bsr_min = bsr_filter.value
    #         if bsr_filter.comparison_operator in [FSComparisonOperator.LESS_OR_EQUAL, FSComparisonOperator.LESS_THAN]:
    #             bsr_max = bsr_filter.value
    #     # filter only upload date filters
    #     upload_filters=list(filter(lambda x: x.field == MBAShirtOrderByField.UPLOAD or x.field=="takedown", simple_query_filters))
    #     self.load_data_with_order_by_upload_date(bsr_min, bsr_max, load_batch_in_cache_size, get_batch_request_id, simple_query_filters=upload_filters, doc_id_cursor=doc_id_cursor)

    def load_with_different_order_bys(self, previous_order_by: str, load_batch_in_cache_size:int, get_batch_request_id: str, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, doc_id_cursor=None, bsr_max=None):
        """ This function is called if not enough matching data can be loaded by previous_order_by
            process:
                1. Get all order by strings that make sense with given simple:query_filters (exclude previous_order_by)
                loop over order bys:
                    2. Get filters which are possible with order by of 1.
                    3. If previous_order_by is bsr_last change bsr_range filter to doc_id_cursor bsr_last value as bsr_min
                    4. Load new batch data for each order by of 1.
        """
        if simple_query_filters==None: return None
        # 1. get order by filters
        order_by_filters: List[FSSimpleFilterQuery] = list(filter(lambda s_filter: s_filter.field in MBAShirtOrderByField.to_list() and s_filter.field != previous_order_by, simple_query_filters))
        for order_by_filter in order_by_filters:
            order_by = order_by_filter.field
            order_by_direction: OrderByDirection = MBA_SHIRT_ORDERBY_DICT[order_by].direction
            # 2. Get fs possible filters
            fs_possible_query_filters = self.get_fs_possible_filters(simple_query_filters, MBA_SHIRTS_COLLECTION_INDEXES, order_by, order_by_direction)
            print(f"Load data with different order_by {order_by} with direction {order_by_direction} and filter {fs_possible_query_filters}")
            # 3. change bsr_range filter
            if previous_order_by == MBAShirtOrderByField.BSR:
                bsr_range_filter = list(filter(lambda s_filter: s_filter.field == FSMbaShirtsIndexField.BSR_RANGE, fs_possible_query_filters))
                if bsr_range_filter != []:
                    # call by reference change of fs_possible_query_filters
                    if doc_id_cursor: # use bsr_min from last cursor or previous min value but maximum of 2 (make sure bsr does not jump to high value)
                        bsr_range_filter[0].value = get_bsr_range_list((bsr2bsr_range_value(self.doc_id2cache_doc[doc_id_cursor].fs_document[FSMbaShirtsIndexField.BSR]), bsr_max),2)
                    else:
                        bsr_range_filter[0].value = get_bsr_range_list((bsr_range_filter[0].value[0], bsr_max),2)


            # 4. load new batches
            # TODO: doc_id_cursor is nat the last one fetched from last different order_by fetch of batch data. How to handle this problem, to get right cursor?
            self.load_batch_in_cache(batch_size=load_batch_in_cache_size, order_by=order_by,
                                     order_by_direction=order_by_direction, simple_query_filters=fs_possible_query_filters,
                                     tmp_id=get_batch_request_id, doc_id_cursor=doc_id_cursor)

    def get_batch_by_cache(self, batch_size, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None,
                           load_batch_in_cache_size=100, order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, doc_id_cursor: Optional[str]=None, bsr_max: Optional[int]=None) -> List[FSDocument]:
        """ Try to load data from cache filtered by simple_query_filters

            Process:
                0. Check if filters are for simple order by sorting (filter_docs_by_order_by_key/**B1**)
                1. If enough data is in cache which matches filter conditions (**B1** is important) -> return batch of FSDocument
                    1.1 Iterate over self.doc_id2cache_doc and filter by all simple_query_filters (TODO: Might must be sorted by order by)
                    1.2 If **doc_id_cursor** (e.g. asin) is provided -> return False until cursor appears
                    1.3 If **B1** is True and enough docs found (> batch_size) -> return False for all following docs (faster)
                    1.4 cacher_doc knows if he is part of an **B1** condition.
                2. If not enough data is in cache:
                    2.1 Get filters which are possible to filter within Firestore/index available (reduce simple_query_filters)
                    2.2 Load new batch of data
                        * Track if cacher_doc is part of **B1**
                3. start with 1 again
            If no data in batch left, new data will be loaded to Cache object

            batch_size: Number of Documents that should be returned, e.g. 12
        """
        number_docs_in_cache_start = len(self.doc_id2cache_doc.keys())
        # TODO: exclude statistics stuff to be more automatic in background and not in this function
        get_batch_request_id = uuid.uuid4().hex
        self._uuid2statistics[get_batch_request_id] = {"nr_fs_reads": 0}
        fs_possible_query_filters = self.get_fs_possible_filters(simple_query_filters, MBA_SHIRTS_COLLECTION_INDEXES, order_by, order_by_direction)
        filter_takedown_value: Optional[bool] = self.get_value_by_filters(simple_query_filters, "takedown")
        filter_docs_by_order_by_key = self.is_simple_fs_order_by_query(simple_query_filters, order_by, order_by_direction, filter_takedown=filter_takedown_value)
        filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(simple_query_filters, batch_size=batch_size, order_by=order_by, order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key, doc_id_cursor=doc_id_cursor)
        loop_counter = 0
        while len(filtered_cacher_docs) < batch_size:
            self.load_batch_in_cache(batch_size=load_batch_in_cache_size, order_by=order_by, order_by_direction=order_by_direction, simple_query_filters=fs_possible_query_filters, tmp_id=get_batch_request_id, doc_id_cursor=doc_id_cursor)
            filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(simple_query_filters, batch_size=batch_size, order_by=order_by, order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key, doc_id_cursor=doc_id_cursor)
            loop_counter += 1
            # break condition to prevent endlees fetching of new data (costs per read)
            if self._max_load_new_batch_nr <= loop_counter:
                # TODO: Try to get data with order by upload_date, if upload filter exissimple_query_filtersts
                #self.load_data_with_order_by_upload_date_with_filters(load_batch_in_cache_size, get_batch_request_id, simple_query_filters=simple_query_filters, doc_id_cursor=doc_id_cursor)
                self.load_with_different_order_bys(order_by, load_batch_in_cache_size, get_batch_request_id, simple_query_filters, doc_id_cursor, bsr_max)
                filter_docs_by_order_by_key = False
                # prevent endless loop and get data independend of order by restriction (filters are applied anyway)
                filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(simple_query_filters, batch_size=batch_size,
                                                                order_by=order_by,
                                                                order_by_direction=order_by_direction,
                                                                filter_docs_by_order_by_key=filter_docs_by_order_by_key,doc_id_cursor=doc_id_cursor)
                break
        print(len(self.doc_id2cache_doc.keys()) - number_docs_in_cache_start, "Number of documents added to cache.", "Statistics:", self._uuid2statistics[get_batch_request_id])

        if filter_docs_by_order_by_key: # in case wie used only order_by docs order is already right
            return [cacher_doc.fs_document for cacher_doc in filtered_cacher_docs[0:batch_size]]
        else:
            # sort docs first and than get desired batch_size
            if order_by:
                # sort by reference
                filtered_cacher_docs.sort(key=lambda cacher_doc: cacher_doc.fs_document[order_by], reverse=order_by_direction == OrderByDirection.DESC)
            return [cacher_doc.fs_document for cacher_doc in filtered_cacher_docs[0:batch_size]]

    def get_ordered_cacher_docs(self, order_by: Optional[str], reverse=False) -> List[CacherDocument]:
        ordered_cacher_docs = list(self.doc_id2cache_doc.values())
        ordered_cacher_docs.sort(key=lambda cacher_doc: cacher_doc.fs_document[order_by],
                                  reverse=reverse)
        return ordered_cacher_docs

    def get_order_by_cursor(self, doc_id: Optional[str], order_by: Optional[str], reverse=False) -> Optional[Union[float, int, str, datetime, bool]]:
        if doc_id not in self.doc_id2cache_doc or doc_id==None or order_by==None:
            return None
        ordered_cacher_docs = self.get_ordered_cacher_docs(order_by, reverse=reverse)
        return next((x.fs_document[order_by] for x in ordered_cacher_docs if x.doc_id == doc_id), None)
