from datetime import datetime
from google.cloud import firestore
from mwfunctions.pydantic import OrderByDirection
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Union, Dict
import logging
import google
import uuid

from mwfunctions.exceptions import log_suppress
from mwfunctions import environment
from mwfunctions.pydantic.firestore.firestore_classes import FSDocument, FSMBAShirtOrderBy
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
    ARRAY_CONTAINS="array-contains"
    ARRAY_CONTAINS_ANY="array-contains-any"
    IN="in"
    NOT_IN="not-in"

class FSSimpleFilterQuery(BaseModel):
    field: str = Field(description="FS field to filter on", example="active")
    comparison_operator: FSComparisonOperator = Field(description="Query operator, e.g. '>', '==', '<'. https://firebase.google.com/docs/firestore/query-data/queries#query_operators", example="==")
    value: Union[str,int,datetime,float,list] = Field(description="Value which should match the condition of operator. E.g. True. Value should have right type.", example=True)

def filter_by_fs_comparison_operator(field_value: Union[str,int,datetime,float,list], comparison_operator:FSComparisonOperator, compare_value: Union[str,int,datetime,float,list]):
    def dt_to_integer(dt_time):
        return 10000 * dt_time.year + 100 * dt_time.month + dt_time.day

    # make operations possible for datetime objects
    if isinstance(field_value, datetime):
        field_value = dt_to_integer(field_value)
    if isinstance(compare_value, datetime):
        compare_value = dt_to_integer(compare_value)

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
    elif comparison_operator == FSComparisonOperator.IN:
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        return field_value in compare_value
    elif comparison_operator == FSComparisonOperator.NOT_IN:
        assert isinstance(compare_value, list), f"compare_value {compare_value} must be of type list but is {type(compare_value)}"
        return field_value not in compare_value
    else:
        raise NotImplementedError


def get_collection_query(collection_path, simple_query_filters: Optional[List[FSSimpleFilterQuery]] = None, client=None):
    """ Returns a collection reference (query) with optional query_filters (e.g. where or in or </> conditions)

    Args:
        collection_path (str): Collection Firestore path
        simple_query_filters: List of FSSimpleFilterQuery which will be queried in order.
        client (google.cloud.firestore_v1.client.Client): Firestore client

    Returns:
        collection reference object

    """
    simple_query_filters = simple_query_filters or []

    # No "/" at start for the python api
    collection_path = collection_path[1:] if collection_path[0] == "/" else collection_path
    assert (len(collection_path.split(
        "/")) % 2) != 0, "path needs be a collection path with odd number of '/' seperated elements"
    client = client if client else create_client()
    query = client.collection(collection_path)

    # if simple_query_filters are provided multiple where conditions are appended to firestore query
    for simple_query_filter in simple_query_filters:
        query = query.where(simple_query_filter.field, simple_query_filter.comparison_operator, simple_query_filter.value)
    return query

def get_docs_iterator(collection_path, simple_query_filters: Optional[List[FSSimpleFilterQuery]] = None, client=None):
    """ Return all documents of given path/collection path as a generator.

    Args:
        collection_path (str): Collection Firestore path
        simple_query_filters: List of FSSimpleFilterQuery which will be queried in order.
        client (google.cloud.firestore_v1.client.Client): Firestore client

    Returns:
        iterable
    """
    query = get_collection_query(collection_path, simple_query_filters=simple_query_filters, client=client)
    return query.stream()


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


class OrderByCursor(object):
    def __init__(self, cursor):
        self.cursor: Union[str,int,datetime,float] = cursor # can be of any type and depends on order by field in FS

    @staticmethod
    def get_order_by_key(order_by, order_by_direction):
        if order_by and order_by_direction:
            return f"{order_by}_{order_by_direction}"
        else:
            return None

class CacherDocument(object):
    def __init__(self, fs_document, order_by_key=None, order_by=None, order_by_direction: OrderByDirection =None):
        order_by_key = order_by_key if order_by_key else f"{order_by}_{order_by_direction}" if (order_by and order_by_direction) else None
        self.order_by_key_list: list=[order_by_key] if order_by_key else []
        self.fs_document: FSDocument = fs_document

    def add_order_by_key(self, order_by_key=None, order_by=None, order_by_direction: OrderByDirection =None):
        assert order_by_key or (order_by and order_by_direction), "Either order_by_key or (order_by and order_by_direction) must be set"
        order_by_key = order_by_key if order_by_key else OrderByCursor.get_order_by_key(order_by, order_by_direction)
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

        self.client = client if client else create_client()
        self.pydantic_class: FSDocument = pydantic_class
        self.collection_path = collection_path
        self.doc_id2fs_doc: Dict[str, CacherDocument] = {}


        #self.fs_document_list: List[FSDocument] = []


    def load_batch_in_cache(self, batch_size=100, order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC):
        # this should word without any index because no filter is required, only order by
        order_by_key = OrderByCursor.get_order_by_key(order_by, order_by_direction)
        order_by_cursor = self._order_by_cursors[order_by_key].cursor if order_by_key and order_by_key in self._order_by_cursors else None
        docs_iter = get_docs_batch(self.collection_path, limit=batch_size, order_by=order_by, direction=order_by_direction, client=self.client, start_after=order_by_cursor)
        doc_pydantic = None
        for doc in docs_iter:
            try:
                # Transform doc to pydantic
                doc_pydantic: FSDocument = self.pydantic_class.parse_fs_doc_snapshot(doc)
                doc_pydantic.set_fs_col_path(self.collection_path)
                # Add to CacherDocument if not exists or add order by data to document
                if doc.id not in self.doc_id2fs_doc:
                    self.doc_id2fs_doc[doc.id] = CacherDocument(doc_pydantic, order_by=order_by, order_by_direction=order_by_direction)
                else:
                    self.doc_id2fs_doc[doc.id].add_order_by_key(order_by=order_by, order_by_direction=order_by_direction)

            except ValidationError as e:
                print(f"Could not parse document with id {doc.id} to pydantic class", str(e))

        # update order by cursor
        if order_by_key not in self._order_by_cursors:
            if doc_pydantic and order_by:
                self._order_by_cursors[order_by_key] = OrderByCursor(doc_pydantic[order_by])
        else:
            if doc_pydantic and order_by:
                self._order_by_cursors[order_by_key].cursor = doc_pydantic[order_by]

    @staticmethod
    def filter_fs_document_by_simple_query_filter(fs_document: FSDocument, simple_query_filter: FSSimpleFilterQuery) -> bool:
        fs_field_value = fs_document[simple_query_filter.field]
        return filter_by_fs_comparison_operator(fs_field_value, simple_query_filter.comparison_operator, simple_query_filter.value)

    def filter_fs_document_by_simple_query_filters(self, cacher_doc: CacherDocument, filter_true_counter_id,
                                                   simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, batch_size=None,
                                                   order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, filter_docs_by_order_by_key:bool=True) -> bool:
        # if enough cached docs are found (i.e. more than batch_size requires) we dont have to check ither docs and return False
        # If not filter_docs_by_order_by_key, all docs should be filtered to sort afterwards by order by and get right docs
        if filter_docs_by_order_by_key and batch_size and self._uuid2filter_true_counter[filter_true_counter_id] > batch_size: return False
        # filter only those which are connected to order by key
        if filter_docs_by_order_by_key and order_by and order_by_direction:
            if not cacher_doc.is_part_of_order_by(OrderByCursor.get_order_by_key(order_by, order_by_direction)): return False
        if simple_query_filters == None or simple_query_filters == []:
            self._uuid2filter_true_counter[filter_true_counter_id] += 1
            return True
        filter_bool = all([FsDocumentsCacher.filter_fs_document_by_simple_query_filter(cacher_doc.fs_document, simple_query_filter) for simple_query_filter in simple_query_filters])
        if filter_bool: self._uuid2filter_true_counter[filter_true_counter_id] += 1
        return filter_bool

    def filter_fs_doc_dict(self, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, batch_size=None,
                           order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, filter_docs_by_order_by_key=True) -> List[CacherDocument]:
        # TODO:  if batch_size is provided only batch_size number products should be filtered (prvent all data to be filtered every time)
        # filter is only refernece to filter_fs_document_by_simple_query_filters function
        filter_true_counter_id = uuid.uuid4().hex
        self._uuid2filter_true_counter[filter_true_counter_id] = 0
        fs_document_filter = filter(lambda cacher_doc:
                                    self.filter_fs_document_by_simple_query_filters(cacher_doc, filter_true_counter_id, simple_query_filters, order_by=order_by, batch_size=batch_size,
                                    order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key), self.doc_id2fs_doc.values())
        cacher_docs: List[CacherDocument] = list(fs_document_filter)
        del self._uuid2filter_true_counter[filter_true_counter_id]
        return cacher_docs

    def get_batch_by_cache(self, batch_size, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None,
                           load_batch_in_cache_size=100, order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC) -> List[FSDocument]:
        """ Try to load data from cache filtered by simple_query_filters
            If no data in batch left, new data will be loaded to Cache object

            batch_size: Number of Documents that should be returned, e.g. 12
        """
        filter_docs_by_order_by_key = True
        filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(simple_query_filters, batch_size=batch_size, order_by=order_by, order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key)
        loop_counter = 0
        while len(filtered_cacher_docs) < batch_size:
            self.load_batch_in_cache(batch_size=load_batch_in_cache_size, order_by=order_by, order_by_direction=order_by_direction)
            filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(simple_query_filters, batch_size=batch_size, order_by=order_by, order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key)
            loop_counter += 1
            if self._max_load_new_batch_nr <= loop_counter:
                filter_docs_by_order_by_key = False
                # prevent endless loop and get data independend of order by restriction (filters are applied anyway)
                filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(simple_query_filters, batch_size=batch_size,
                                                                order_by=order_by,
                                                                order_by_direction=order_by_direction,
                                                                filter_docs_by_order_by_key=filter_docs_by_order_by_key)
                break

        if filter_docs_by_order_by_key: # in case wie used only order_by docs order is already right
            return [cacher_doc.fs_document for cacher_doc in filtered_cacher_docs[0:batch_size]]
        else:
            # sort docs first and than get desired batch_size
            if order_by:
                # sort by reference
                filtered_cacher_docs.sort(key=lambda cacher_doc: cacher_doc.fs_document[order_by], reverse=order_by_direction == OrderByDirection.DESC)
            return [cacher_doc.fs_document for cacher_doc in filtered_cacher_docs[0:batch_size]]
