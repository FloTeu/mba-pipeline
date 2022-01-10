import copy
import uuid
from datetime import datetime
from typing import Optional, Union, List, Dict

from mwfunctions.cloud.firestore import create_client, get_docs_batch
from mwfunctions.cloud.firestore.commons import filter_simple_query_filters, filter_by_fs_comparison_operator, \
    filter_simple_query_filters_by_fs_indexes, FSComparisonOperator, FSSimpleFilterQuery, OrderByDirection
from mwfunctions.profiling import log_time
from mwfunctions.pydantic import FSDocument
from mwfunctions.pydantic.firestore.mba_shirt_classes import FSMBAShirt, MBAShirtOrderByField, MBA_SHIRT_ORDERBY_DICT
from mwfunctions.pydantic.firestore.utils import bsr2bsr_range_value, get_bsr_range_list
from mwfunctions.pydantic.firestore.indexes import FSMbaShirtsIndexField, FSIndex, MBA_SHIRTS_COLLECTION_INDEXES
from pydantic import BaseModel, Field, validator, ValidationError
from itertools import compress

class OrderByCursor(object):
    def __init__(self, cursor, order_by_key, keywords_stem_list: Optional[list]=None, filter_takedown_value: Optional[bool]=None):
        self.cursor: Union[str,int,datetime,float] = cursor # can be of any type and depends on order by field in FS
        self.keywords_stem_list = keywords_stem_list # list of stemmed keywords which where used to get data from FS
        self.filter_takedown_value = filter_takedown_value
        self.order_by_key = order_by_key

    @staticmethod
    def get_order_by_key(order_by, order_by_direction, search_key_stem_list: Optional[list]=None, filter_takedown: Optional[bool]=None):
        # Asumption: if keyword search, fs is not used with order by but FSMbaShirtsIndexField.BSR_RANGE increments in loop and therefore its ordered indirectly
        if search_key_stem_list:
            order_by = FSMbaShirtsIndexField.BSR_RANGE
            order_by_direction = OrderByDirection.ASC

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


class BatchLoadRequestStatistics(BaseModel):
    nr_fs_reads: Optional[int] = 0


class BatchLoadRequest(BaseModel):
    _load_batch_counter: int = 0
    id: str = uuid.uuid4().hex
    batch_size: int = 100
    order_by: Optional[str] = None
    order_by_direction: Optional[OrderByDirection] = OrderByDirection.ASC
    simple_query_filters: Optional[List[FSSimpleFilterQuery]] = Field(None, description="List of FS possible simple query filters")
    local_simple_query_filters: Optional[List[FSSimpleFilterQuery]] = Field(None, description="List of simple query filters which should be applied after FS query to filter cacher docs. Can be reduced e.g. by bsr_last_range filter in comparison to all_simple_query_filters")
    all_simple_query_filters: Optional[List[FSSimpleFilterQuery]] = Field(None, description="List of all simple query filters, which did not have to be possible as FS filters, but can be used afterwards for filtering")
    order_by_key: Optional[str] = None
    doc_id_cursor: Optional[str] = None
    bsr_last_range_max: Optional[int] = Field(None, description="Max value of bsr_last_range filter")
    bsr_last_range_update_counter: int = 0
    statistics: BatchLoadRequestStatistics = BatchLoadRequestStatistics()
    verbose: bool = Field(False, description="Whether to show more information in console")


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.is_keyword_search():
            self.order_by = None
            self.order_by_direction = None
            self.doc_id_cursor = None  # doc_id_cursor is not used for filter cacher docs if keyword search. Order depends on FS internal order system not order by from us.
            self.order_by_key = FsDocumentsCacher.get_order_by_key_by_filters(self.order_by, self.order_by_direction, self.simple_query_filters)
        # create new id after every init
        self.id = uuid.uuid4().hex

    @staticmethod
    def get_fs_possible_filters(simple_query_filters: Optional[List[FSSimpleFilterQuery]], fs_collection_indexes: List[FSIndex], order_by, order_by_direction: OrderByDirection, is_keyword_search=False) -> Optional[List[FSSimpleFilterQuery]]:
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
        fs_possible_filters: List[FSSimpleFilterQuery] = copy.deepcopy(filter_simple_query_filters_by_fs_indexes(simple_query_filters, fs_collection_indexes, order_by, order_by_direction, is_keyword_search=is_keyword_search))
        if fs_possible_filters == None: return None
        # TODO: filter simple_query_filters by available fs_collection_indexes
        # is_keyword_search = any([x.field == "keywords_stem_list" for x in simple_query_filters])
        # fs_possible_filters = copy.deepcopy(list(filter(lambda x: x.field == order_by or x.field == "keywords_stem_list" or x.field == "takedown" or x.field == "bsr_last_range", simple_query_filters)))
        # change array contains all to array contains any, because FS cannot query ARRAY_CONTAINS_ALL operation
        for x in fs_possible_filters:
            if x.comparison_operator == FSComparisonOperator.ARRAY_CONTAINS_ALL:
                x.comparison_operator = FSComparisonOperator.ARRAY_CONTAINS_ANY

        # IN Operatiosn are allowed to have max 10 value sin list
        if fs_possible_filters:
            for comp_op in [FSComparisonOperator.IN, FSComparisonOperator.NOT_IN]:
                in_operation_filters: List[FSSimpleFilterQuery] = filter_simple_query_filters(fs_possible_filters, comparison_operator=comp_op)
                for in_operation_filter in in_operation_filters:
                    in_operation_filter.value = in_operation_filter.value[0:10]

        return fs_possible_filters

    @validator("order_by_key", always=True)
    def set_order_by_key(cls, order_by_key, values):
        return order_by_key if order_by_key else FsDocumentsCacher.get_order_by_key_by_filters(values["order_by"], values["order_by_direction"],
                                                    values["simple_query_filters"])

    @validator("all_simple_query_filters")
    def set_simple_query_filters(cls, all_simple_query_filters, values):
        # if not set already, try to initialize simple_query_filters with all_simple_query_filters
        if "simple_query_filters" not in values or not values["simple_query_filters"]:
            values["simple_query_filters"] = cls.get_fs_possible_filters(all_simple_query_filters, MBA_SHIRTS_COLLECTION_INDEXES, values["order_by"], values["order_by_direction"], is_keyword_search=cls.is_keyword_search_static(all_simple_query_filters))
        if "local_simple_query_filters" not in values or not values["local_simple_query_filters"]:
            # drop bsr_last_range for final filters
            # TODO: check if this leads to problem for other cases then keyword search
            values["local_simple_query_filters"] = filter_simple_query_filters(all_simple_query_filters, field=FSMbaShirtsIndexField.BSR_RANGE, use_equal_compare=False)
            # copy.deepcopy(all_simple_query_filters)
        return all_simple_query_filters

    def inc_statistics_nr_fs_reads(self):
        self.statistics.nr_fs_reads += 1

    def inc_load_batch_counter(self):
        self._load_batch_counter = self._load_batch_counter + 1

    def get_load_batch_counter(self):
        return self._load_batch_counter

    def add_bsr_last_range_filter(self, bsr_last_range_min=0):
        assert len(filter_simple_query_filters(self.simple_query_filters,field=FSMbaShirtsIndexField.BSR_RANGE)) == 0, "bsr_last_range filter already exists"
        # if no bsr_last_range filter is set this function can be called to init one starting at 1
        self.simple_query_filters.append(FSSimpleFilterQuery(field=FSMbaShirtsIndexField.BSR_RANGE, comparison_operator=FSComparisonOperator.IN, value=[bsr_last_range_min]))

    def get_bsr_last_range_min_by_filter(self, bsr_last_range_filters=None) -> Optional[int]:
        bsr_range_list_filters = bsr_last_range_filters if bsr_last_range_filters else filter_simple_query_filters(self.simple_query_filters,
                                                             field=FSMbaShirtsIndexField.BSR_RANGE)
        if len(bsr_range_list_filters) > 0 and len(bsr_range_list_filters[0].value) > 0:
            return bsr_range_list_filters[0].value[0]

    def get_bsr_last_range_max_by_filter(self, bsr_last_range_filters=None) -> Optional[int]:
        bsr_range_list_filters = bsr_last_range_filters if bsr_last_range_filters else filter_simple_query_filters(self.simple_query_filters,
                                                             field=FSMbaShirtsIndexField.BSR_RANGE)
        if len(bsr_range_list_filters) > 0 and len(bsr_range_list_filters[0].value) > 0:
            return bsr_range_list_filters[0].value[-1]

    def update_bsr_last_range_filter(self, list_size, bsr_last_range_min=None, update_bsr_min_with_bsr_last_range_max=True):
        # uses bsr_min if exists otherwise bsr_range_list_filters[0].value[-1] + 1 if update_bsr_min_with_bsr_last_range_max else bsr_range_list_filters[0].value[0]
        # call by reference change of simple_query_filters
        # TODO: set initial bsr_range filter by order by cursor
        bsr_range_list_filters = filter_simple_query_filters(self.simple_query_filters,
                                                             field=FSMbaShirtsIndexField.BSR_RANGE)
        # either use provided bsr_min or inc. or first or last element of bsr_range_list_filters depending of update_bsr_min_with_bsr_last_range_max
        if not bsr_last_range_min:
            if update_bsr_min_with_bsr_last_range_max and len(bsr_range_list_filters) > 0 and len(
                    bsr_range_list_filters[0].value) > 0:
                bsr_last_range_min = self.get_bsr_last_range_max_by_filter(bsr_range_list_filters) + 1
            elif len(bsr_range_list_filters) > 0 and len(bsr_range_list_filters[0].value) > 0:
                bsr_last_range_min = self.get_bsr_last_range_min_by_filter(bsr_range_list_filters)

        if len(bsr_range_list_filters) > 0:
            bsr_range_list_filters[0].value = get_bsr_range_list((bsr_last_range_min, self.bsr_last_range_max), min(list_size, 10))
            self.bsr_last_range_update_counter += 1
        else:
            bsr_last_range_min = bsr_last_range_min if bsr_last_range_min else 0
            self.add_bsr_last_range_filter(bsr_last_range_min=bsr_last_range_min)

    @staticmethod
    def is_keyword_search_static(simple_query_filters):
        return any(["keywords_stem" in x.field for x in simple_query_filters])

    def is_keyword_search(self):
        return self.is_keyword_search_static(self.simple_query_filters) if self.simple_query_filters else False

    def stop_load_keyword_search_batches(self) -> bool:
        # break loop if bsr_min is higher than bsr_max or no bsr_range_filters list exists anymore
        bsr_range_filters = filter_simple_query_filters(self.simple_query_filters,
                                                        field=FSMbaShirtsIndexField.BSR_RANGE)
        bsr_last_range_min = FsDocumentsCacher.get_bsr_last_range_min_by_filters(self.simple_query_filters)
        if len(bsr_range_filters) == 0 or (len(bsr_range_filters) > 0 and len(bsr_range_filters[0].value) == 0) or (
                bsr_last_range_min and self.bsr_last_range_max and bsr_last_range_min >= self.bsr_last_range_max):
            return True
        else:
            return False

    class Config:
        extra = "allow"

    def is_simple_fs_order_by_query(self, order_by_cursors: Dict[str, OrderByCursor]): #, use_fs_simple_query_filters=True):
        """ Whether query task is a simple order by query (i,e, order by query from starting point without further filerings except takedown):
            Rules:
                * order by is included
                * Other filters are only takedown filter
                + order_by cursor matches order_by_filter start

            considered are only get_fs_possible_filters

            Not simple_order_by_queries are for example:
                * Search key query
        """
        simple_query_filters = self.simple_query_filters #if use_fs_simple_query_filters else self.all_simple_query_filters
        # TODO: order_by None could also be a simple order_by_query but alphabetically (i.e. FS infrsatructure sorts docs)
        if self.order_by==None: return False
        if simple_query_filters==None and self.order_by!=None: return True
        filter_takedown: Optional[bool] = FsDocumentsCacher.get_value_by_filters(simple_query_filters, "takedown")
        order_by_key = OrderByCursor.get_order_by_key(self.order_by, self.order_by_direction, filter_takedown=filter_takedown)
        fs_possible_filters: Optional[List[FSSimpleFilterQuery]] = BatchLoadRequest.get_fs_possible_filters(simple_query_filters, MBA_SHIRTS_COLLECTION_INDEXES, self.order_by, self.order_by_direction)
        numer_of_filters_not_takedown_or_order_by = len(list(filter(lambda x: x.field!=self.order_by and x.field!="takedown", fs_possible_filters)))
        if len(fs_possible_filters) == 0: return True
        # CHeck if contains only order_by start or cursor or takedown_filter (exclude filters from other direction e.g. high bsr_max value)
        def does_comparison_operator_matches_direction(comparison_operator: FSComparisonOperator, order_by_direction: OrderByDirection):
            if comparison_operator in [FSComparisonOperator.GREATER_THAN, FSComparisonOperator.GREATER_OR_EQUAL] and order_by_direction == OrderByDirection.ASC:
                return True
            elif comparison_operator in [FSComparisonOperator.LESS_THAN, FSComparisonOperator.LESS_OR_EQUAL] and order_by_direction == OrderByDirection.DESC:
                return True
            else:
                return False
        order_by_filters_default_direction = list(filter(lambda x: x.field==self.order_by and does_comparison_operator_matches_direction(x.comparison_operator,  MBA_SHIRT_ORDERBY_DICT[self.order_by].direction), fs_possible_filters))
        # case: only order by filters and start from beginning
        if numer_of_filters_not_takedown_or_order_by == 0 and len(order_by_filters_default_direction) == 0: return True
        # TODO: check if order_by_filters[0].value == self._order_by_cursors[order_by_key].cursor works
        # no order by filter is considered like from start filter
        if len(order_by_filters_default_direction) == 0:
            is_start_or_cursor_order_by_filter = True
        else:
            is_start_value = order_by_filters_default_direction[0].value == MBA_SHIRT_ORDERBY_DICT[self.order_by].start_value
            is_cursor_value = order_by_filters_default_direction[0].value == order_by_cursors[order_by_key].cursor if order_by_key in order_by_cursors else False
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


class BatchLoadResponse(BaseModel):
    last_fs_doc: Optional[FSMBAShirt]=None
    highest_bsr_last_range: Optional[int]=None
    full_batch_size_loaded: bool

    @validator("last_fs_doc", allow_reuse=True)
    def validate_last_fs_doc(cls, last_fs_doc, values):
        # Dont know why, but pydantic parses field last_fs_doc to dict and not to pydantic obj
        if last_fs_doc:
            last_fs_doc = FSMBAShirt.parse_obj(last_fs_doc)
        return last_fs_doc

    def update_full_batch_size_loaded(self, batch_load_request: BatchLoadRequest, nr_docs_loaded):
        self.full_batch_size_loaded = batch_load_request.batch_size == nr_docs_loaded

        # if full_batch_size_loaded is False (no more shirts left) and highest_bsr_last_range is at max (99)
        #   -> We know that no more data can be loaded.
        # Therefore -1 is set to highest_bsr_last_range to indicate end of keyword seacrh
        if not self.full_batch_size_loaded and self.highest_bsr_last_range == 99:
            self.highest_bsr_last_range = -1

    def update_last_fs_doc(self, fs_doc: FSMBAShirt):
        self.last_fs_doc = FSMBAShirt.parse_obj(fs_doc)
        # update highest_bsr_last_range
        if not self.highest_bsr_last_range:
            self.highest_bsr_last_range = fs_doc[FSMbaShirtsIndexField.BSR_RANGE]
        elif fs_doc[FSMbaShirtsIndexField.BSR_RANGE] > self.highest_bsr_last_range:
            self.highest_bsr_last_range = fs_doc[FSMbaShirtsIndexField.BSR_RANGE]


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
        # TODO: log all requests filters -> Goal. check if filter was already applied If yes take existing cache docs if No. get new batch loads
        self._all_batch_load_requests: List[BatchLoadRequest] = []
        self._order_by_cursors: Dict[str, OrderByCursor] = {} # cursor is updated each time cacher is filled with new data and defines last element got from FS. Key is a order by key defined by get_order_by_key
        self._max_load_new_batch_nr = 5 # maximum number of loops to prevent endless fetch of new data
        self._uuid2filter_true_counter = {}
        self._uuid2doc_id_found = {} # Whether cursor for doc id was found or not
        #self._uuid2statistics: Dict[str, dict] = {}
        self._uuid2order_by_cursor: Dict[str, Dict[str, Union[float, int, str, datetime]]] = {} # {tmp_id: {order_by_str: order_by_cursor}}
        self._uuid2bsr_range_min: Dict[str, int] # {tmp_id: bsr_range starting value as int}

        self.client = client if client else create_client()
        self.pydantic_class: FSDocument = pydantic_class
        self.collection_path = collection_path
        self.doc_id2cache_doc: Dict[str, CacherDocument] = {}

    def update_order_by_cursors(self, order_by_key, order_by, batch_load_response: BatchLoadResponse, keywords_stem_list: Optional[list]=None, filter_takedown_value: Optional[bool]=None):
        if batch_load_response.last_fs_doc and order_by:
            cursor_value = batch_load_response.highest_bsr_last_range if keywords_stem_list else batch_load_response.last_fs_doc[order_by]
            if order_by_key not in self._order_by_cursors:
                self._order_by_cursors[order_by_key] = OrderByCursor(cursor_value, order_by_key, keywords_stem_list=keywords_stem_list, filter_takedown_value=filter_takedown_value)
            else:
                self._order_by_cursors[order_by_key].cursor = cursor_value



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

    @staticmethod
    def get_order_by_key_by_filters(order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None) -> str:
        search_key_stem_list: Optional[list] = FsDocumentsCacher.get_keyword_stem_list_by_filters(simple_query_filters)
        filter_takedown_value: Optional[bool] = FsDocumentsCacher.get_value_by_filters(simple_query_filters, "takedown")
        return OrderByCursor.get_order_by_key(order_by, order_by_direction, search_key_stem_list=search_key_stem_list, filter_takedown=filter_takedown_value)

    def load_batch_in_cache(self, batch_load_request: BatchLoadRequest) -> BatchLoadResponse:
        """ Loads new batch of FS documents in cache.
            Returns last document added to to cache as pydantic object.
            Be carefull with simple_query_filter, since indexes might be needed. order by filtering is always possible

        """
        # TODO: if keyword was already queried -> prevent quering same data again
        # keywords_not_queried_already = OrderByCursor.get_keywords_not_queried_already(list(self._order_by_cursors.values()), search_key_stem_list, filter_takedown_value)

        # if keywords stem exists but every keyword was queried already -> use existent order_by_key
        # TODO: Funktioniert keyword search case noch wie gedacht?
        # if search_key_stem_list and (keywords_not_queried_already == None or len(keywords_not_queried_already) == 0):
        #     order_by_key = OrderByCursor.get_order_by_key_by_keyword_list(list(self._order_by_cursors.values()), search_key_stem_list)
        # else:
        # should be true for normal trend page, bsr page etc.
        batch_load_response = BatchLoadResponse(full_batch_size_loaded=False)

        is_simple_fs_order_by_query = batch_load_request.is_simple_fs_order_by_query(self._order_by_cursors)
        order_by_cursor = self.get_order_by_cursor_wrapper(batch_load_request.order_by_key, batch_load_request.id, batch_load_request.order_by, batch_load_request.order_by_direction, is_simple_fs_order_by_query, doc_id_cursor=batch_load_request.doc_id_cursor)

        loop_counter = 0
        # TODO: make IN filters with on element to euqal operator
        # doc_id_cursor can only be used without IN operytors. Therfore we transform IN operators with one element to EQUAL operator
        def make_in_to_equal_filter(simple_query_filters):
            if not simple_query_filters: return simple_query_filters
            changed_simple_query_filters = copy.deepcopy(simple_query_filters)
            in_filters = list(filter(lambda x: x.comparison_operator == FSComparisonOperator.IN, changed_simple_query_filters))
            if in_filters:
                for in_filter in in_filters:
                    if len(in_filter.value) == 1:
                        in_filter.comparison_operator = FSComparisonOperator.EQUAL
                        in_filter.value = in_filter.value[0]
            return changed_simple_query_filters

        docs_iter = get_docs_batch(self.collection_path, limit=batch_load_request.batch_size, order_by=batch_load_request.order_by, direction=batch_load_request.order_by_direction, client=self.client, start_after=order_by_cursor, simple_query_filters=make_in_to_equal_filter(batch_load_request.simple_query_filters), start_after_doc_id=batch_load_request.doc_id_cursor)
        for doc in docs_iter:
            loop_counter += 1
            try:
                batch_load_request.inc_statistics_nr_fs_reads()
                # Transform doc to pydantic
                doc_pydantic: FSDocument = self.pydantic_class.parse_fs_doc_snapshot(doc)
                batch_load_response.update_last_fs_doc(doc_pydantic)
                # only update order by data if query was not filtered
                # TODO: Does algo work like intendend if some docuemnts to not contain order by statement?
                order_by_key_update = batch_load_request.order_by_key if is_simple_fs_order_by_query else None
                # Add to CacherDocument if not exists or add order by data to document
                if doc.id not in self.doc_id2cache_doc:
                    self.doc_id2cache_doc[doc.id] = CacherDocument(doc_pydantic, doc.id, order_by_key=order_by_key_update)
                else:
                    if is_simple_fs_order_by_query:
                        self.doc_id2cache_doc[doc.id].add_order_by_key(order_by_key=order_by_key_update)

            except ValidationError as e:
                if batch_load_request.verbose:
                    print(f"Could not parse document with id {doc.id} to pydantic class", str(e))

        batch_load_response.update_full_batch_size_loaded(batch_load_request, loop_counter)
        return batch_load_response

    def update_cursors(self, batch_load_response: BatchLoadResponse, batch_load_request: BatchLoadRequest, update_search_cursor=False):
        """ Function to update self._uuid2order_by_cursor and self._order_by_cursors.
            doc_pydantic is the last added doc to cache which contains new cursor values
            tmp_id can be for example a get_batch_request_id from one user during one request
        """

        order_by = copy.deepcopy(batch_load_request.order_by)
        order_by_direction = copy.deepcopy(batch_load_request.order_by_direction)
        # order by key can be for example bsr_last_asc or "bsr_last_Vatertag Angler"
        search_key_stem_list: Optional[list] = self.get_keyword_stem_list_by_filters(batch_load_request.simple_query_filters)
        filter_takedown_value: Optional[bool] = self.get_value_by_filters(batch_load_request.simple_query_filters, "takedown")
        is_simple_fs_order_by_query = batch_load_request.is_simple_fs_order_by_query(self._order_by_cursors)
        #batch_load_request.is_simple_fs_order_by_query(self._order_by_cursors)
        # Asumption: if keyword search, fs is not used with order by but FSMbaShirtsIndexField.BSR_RANGE increments in loop and therefore its ordered indirectly
        if search_key_stem_list:
            order_by = FSMbaShirtsIndexField.BSR_RANGE
            order_by_direction = OrderByDirection.ASC

        order_by_key = OrderByCursor.get_order_by_key(order_by, order_by_direction,
                                                      search_key_stem_list=search_key_stem_list,
                                                      filter_takedown=filter_takedown_value)

        # update order by cursor
        if order_by:
            if batch_load_request.id not in self._uuid2order_by_cursor: self._uuid2order_by_cursor[batch_load_request.id] = {}
            self._uuid2order_by_cursor[batch_load_request.id][order_by] = batch_load_response.last_fs_doc[order_by] if batch_load_response.last_fs_doc else \
                self._uuid2order_by_cursor[batch_load_request.id][order_by] if order_by in self._uuid2order_by_cursor[batch_load_request.id] else None
        # Update cursor only if data was loaded without none order by filtering
        if is_simple_fs_order_by_query or (search_key_stem_list and update_search_cursor):
            self.update_order_by_cursors(order_by_key, order_by, batch_load_response,
                                         keywords_stem_list=search_key_stem_list,
                                         filter_takedown_value=filter_takedown_value)

    @staticmethod
    def extract_filter_value_of_dict_filter(simple_query_filter: FSSimpleFilterQuery):
        # e.g. for keyword_stem.`vatertag` -> returns vatertag
        return simple_query_filter.field.split(".`")[-1][0:-1]

    @staticmethod
    def filter_fs_document_by_simple_query_filter(fs_document: FSDocument, simple_query_filter: FSSimpleFilterQuery) -> bool:
        if ".`" in simple_query_filter.field: # case value within dict
            keyword_stem = FsDocumentsCacher.extract_filter_value_of_dict_filter(simple_query_filter)
            fs_field_value = keyword_stem in fs_document[simple_query_filter.field.split(".`")[0]]
        else:
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

        # if enough cached docs are found (i.e. more than batch_size requires) we dont have to check other docs and return False
        # If not filter_docs_by_order_by_key, all docs should be filtered to sort afterwards by order by and get right docs
        if filter_docs_by_order_by_key and batch_size and self._uuid2filter_true_counter[tmp_id] > batch_size: return False

        # filter only those which are connected to order_by_key
        # Normal case: simple order by query without filtering -> no problem (only cacher_doc.is_part_of_order_by() are used)
        # Special case: order by query with multiple filters -> after n batch loads desired output size is not reached because of filters
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
        with log_time(fn_name=f"Sort fs cacher docs dict by {order_by}"):
            ordered_cacher_docs: List[CacherDocument] = self.get_ordered_cacher_docs(order_by, reverse=order_by_direction == OrderByDirection.DESC)

        # make sure doc_id_cursor is only used if cache contains this document
        doc_id_cursor = doc_id_cursor if doc_id_cursor and doc_id_cursor in self.doc_id2cache_doc else None
        tmp_id = uuid.uuid4().hex
        self._uuid2filter_true_counter[tmp_id] = 0
        self._uuid2doc_id_found[tmp_id] = False
        fs_document_filter = filter(lambda cacher_doc:
                                    self.filter_fs_document_by_simple_query_filters(cacher_doc, tmp_id, simple_query_filters, order_by=order_by, batch_size=batch_size, filter_takedown=filter_takedown_value,
                                    order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key, doc_id_cursor=doc_id_cursor), ordered_cacher_docs)
        cacher_docs: List[CacherDocument] = list(fs_document_filter)
        del self._uuid2filter_true_counter[tmp_id]
        del self._uuid2doc_id_found[tmp_id]
        return cacher_docs



    # @staticmethod
    # def get_filtered_filters(simple_query_filters: Optional[List[FSSimpleFilterQuery]], fs_field_str:str) -> Optional[List[FSSimpleFilterQuery]]:
    #     if simple_query_filters == None: return None
    #     return list(filter(lambda x: x.field == fs_field_str if not ".`" in x.field else x.field.split(".`")[0] == fs_field_str, simple_query_filters))

    @staticmethod
    def get_value_by_filters(simple_query_filters: Optional[List[FSSimpleFilterQuery]], fs_field_str:str):
        # returns value of first matching filter by fs_field_str
        if simple_query_filters == None: return None
        filtered_filters=filter_simple_query_filters(simple_query_filters, field=fs_field_str)
        return filtered_filters[0].value if len(filtered_filters) > 0 else None

    @staticmethod
    def get_keyword_stem_list_by_filters(simple_query_filters: Optional[List[FSSimpleFilterQuery]]) -> Optional[list]:
        if simple_query_filters == None: return None
        filtered_filters = filter_simple_query_filters(simple_query_filters, field="keywords_stem")
        if len(filtered_filters) == 0:
            return None
        else:
            return [FsDocumentsCacher.extract_filter_value_of_dict_filter(simple_query_filter) for simple_query_filter in filtered_filters]

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

    def load_with_different_order_bys(self, previous_order_by: str, load_batch_in_cache_size:int, get_batch_request_id: str, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, doc_id_cursor=None, bsr_max=None, is_keyword_search=False):
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
            order_by_direction: OrderByDirection = MBA_SHIRT_ORDERBY_DICT[order_by_filter.field].direction
            batch_load_request = BatchLoadRequest(batch_size=load_batch_in_cache_size, order_by=order_by_filter.field,
                                                  order_by_direction=order_by_direction, all_simple_query_filters=simple_query_filters,
                                                  id=get_batch_request_id, doc_id_cursor=doc_id_cursor,
                                                  bsr_last_range_max=bsr_max)


            print(f"Load data with different order_by {order_by_filter.field} with direction {order_by_direction} and filter {batch_load_request.simple_query_filters}")
            # 3. change bsr_range filter
            if previous_order_by == MBAShirtOrderByField.BSR:
                bsr_range_filter = list(filter(lambda s_filter: s_filter.field == FSMbaShirtsIndexField.BSR_RANGE, batch_load_request.simple_query_filters))
                if bsr_range_filter != []:
                    # call by reference change of fs_possible_query_filters
                    if doc_id_cursor and doc_id_cursor in self.doc_id2cache_doc: # use bsr_min from last cursor or previous min value but maximum of 2 (make sure bsr does not jump to high value)
                        bsr_range_filter[0].value = get_bsr_range_list((bsr2bsr_range_value(self.doc_id2cache_doc[doc_id_cursor].fs_document[FSMbaShirtsIndexField.BSR]), bsr_max),2)
                    else:
                        bsr_range_filter[0].value = get_bsr_range_list((bsr_range_filter[0].value[0], bsr_max),2)

            # 4. load new batches
            # TODO: doc_id_cursor is not the last one fetched from last different order_by fetch of batch data. How to handle this problem, to get right cursor?
            self.load_batch_in_cache(batch_load_request)

    # @staticmethod
    # def get_bsr_last_range_all_filter(simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None):
    #     # returns the FSSimpleFilterQuery of bsr_last_range field for all bsr values that should be filtered (not only those which can be filtered in FS)
    #     bsr_range_list_filters = list(filter(lambda x: x.field==FSMbaShirtsIndexField.BSR_RANGE,simple_query_filters))
    #     bsr_min = bsr_range_list_filters[0].value[0] if len(bsr_range_list_filters) > 0 else None
    #     return FSSimpleFilterQuery(field=FSMbaShirtsIndexField.BSR_RANGE, comparison_operator=FSComparisonOperator.IN, value=get_bsr_range_list((bsr_min,bsr_max),51))

    def get_batch_by_cache(self, batch_size, simple_query_filters: Optional[List[FSSimpleFilterQuery]]=None, is_meta_search=False,
                           load_batch_in_cache_size=100, order_by=None, order_by_direction: OrderByDirection = OrderByDirection.ASC, doc_id_cursor: Optional[str]=None, bsr_range_list_max: Optional[int]=None, page=None) -> List[FSDocument]:
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

        # TODO: what happens if new instance spawns with new cache but user is in page!=1
        number_docs_in_cache_start = len(self.doc_id2cache_doc.keys())
        is_search_from_bsr_last_range_start = self.get_bsr_last_range_min_by_filters(simple_query_filters) == 0 if self.get_bsr_last_range_min_by_filters(simple_query_filters) else False

        # integer representation if max value is None
        bsr_range_list_max_int = bsr_range_list_max if bsr_range_list_max else 51

        # bsr_last_range_all_filter = self.get_bsr_last_range_all_filter(simple_query_filters)
        # keyword search does not use order by but increments bsr_range value to sort values
        # fs_possible_query_filters = self.get_fs_possible_filters(simple_query_filters, MBA_SHIRTS_COLLECTION_INDEXES, order_by, order_by_direction, is_keyword_search=is_keyword_search)
        bsr_last_range_cursor = None

        # init of batch load request
        batch_load_request = BatchLoadRequest(batch_size=load_batch_in_cache_size, order_by=order_by,
                                              order_by_direction=order_by_direction,
                                              all_simple_query_filters=simple_query_filters,
                                              doc_id_cursor=doc_id_cursor,
                                              bsr_last_range_max=bsr_range_list_max)

        max_load_new_batch_nr = self._max_load_new_batch_nr if not batch_load_request.is_keyword_search() else 8

        if batch_load_request.is_keyword_search():
            bsr_last_range_cursor = self.update_beginning_bsr_last_range_filter(batch_load_request, doc_id_cursor, update_only_if_cursor_found=True)

        # check if batch_load_request was already queried in FS. If page is > 1 we can assume that was_batch_load_req_already_queried_in_fs is True
        matching_batch_load_requests_filter = [cached_batch_load_request.simple_query_filters == batch_load_request.simple_query_filters for cached_batch_load_request in self._all_batch_load_requests]
        matching_batch_load_requests = list(compress(self._all_batch_load_requests, matching_batch_load_requests_filter))
        was_batch_load_req_already_queried_in_fs = True if page not in [None, 1] else len(matching_batch_load_requests) > 0

        # try to speed up
        if batch_load_request.is_keyword_search() and len(matching_batch_load_requests) > 0:
            bsr_last_range_cursor = self.update_beginning_bsr_last_range_filter(batch_load_request, doc_id_cursor, matching_batch_load_requests)

        # safe batch request after beginning_bsr_last_range_filter is set
        self._all_batch_load_requests.append(copy.deepcopy(batch_load_request))

        filter_docs_by_order_by_key = batch_load_request.is_simple_fs_order_by_query(self._order_by_cursors)#, use_fs_simple_query_filters=False)
        # check if we already find enough cacher_docs (but only if was_batch_load_req_already_queried_in_fs is True). Otherwise try to get new data from FS
        filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(batch_load_request.local_simple_query_filters, batch_size=batch_size, order_by=order_by, order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key, doc_id_cursor=doc_id_cursor) if was_batch_load_req_already_queried_in_fs else []
        # normal search tried to get 100 docs therefore 50 required meta data docs should be also loaded if they exists.
        # Otherwise this speeds up search because no second load is required
        meta_keyword_search_finished = len(matching_batch_load_requests) > 0 and batch_load_request.is_keyword_search() and batch_size < 100 and is_meta_search

        while len(filtered_cacher_docs) < batch_size and not meta_keyword_search_finished:
            # if last cursor of bsr_last_range indicates that no more data left (-1) we finish loop directly
            if batch_load_request.is_keyword_search() and bsr_last_range_cursor and bsr_last_range_cursor == -1: break

            batch_load_response: BatchLoadResponse = self.load_batch_in_cache(batch_load_request)
            # if keyword search and bsr_last_range filter does not exist, we try to get all data without bsr_range filter (fast and possible for < 100 docs)
            # if we got more than 100, we start with bsr_last_range = [1] filter and increment over loop
            # skip for meta search to make process faster
            if batch_load_request.is_keyword_search() and not is_meta_search:
                bsr_last_range_filters = filter_simple_query_filters(batch_load_request.simple_query_filters,
                                                                     field=FSMbaShirtsIndexField.BSR_RANGE)
                filter_bsr_last_range_max = batch_load_request.get_bsr_last_range_max_by_filter(bsr_last_range_filters)
                contains_more_data_but_has_no_bsr_last_range_filter = len(bsr_last_range_filters) == 0 and page in [
                    None, 1] and batch_load_response.highest_bsr_last_range != -1
                # contains more data if first batch_load request and full batch was loaded
                contains_more_data = page in [
                    None, 1] and batch_load_request.get_load_batch_counter() == 0 and batch_load_response.full_batch_size_loaded #batch_load_response.highest_bsr_last_range not in [-1, filter_bsr_last_range_max, bsr_range_list_max_int-1]
                if contains_more_data_but_has_no_bsr_last_range_filter or contains_more_data: # case keyword earch but not finished
                    bsr_last_range_min = batch_load_request.get_bsr_last_range_min_by_filter()
                    bsr_last_range_min = bsr_last_range_min if bsr_last_range_min else 0
                    #batch_load_request.add_bsr_last_range_filter(bsr_last_range_min=bsr_last_range_min)
                    batch_load_request.update_bsr_last_range_filter(1, bsr_last_range_min=bsr_last_range_min,
                                                 update_bsr_min_with_bsr_last_range_max=False)
                    batch_load_response: BatchLoadResponse = self.load_batch_in_cache(batch_load_request)

            self.update_cursors(batch_load_response, batch_load_request, update_search_cursor=is_search_from_bsr_last_range_start)

            batch_load_request.inc_load_batch_counter()
            filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(batch_load_request.local_simple_query_filters, batch_size=batch_size, order_by=order_by, order_by_direction=order_by_direction, filter_docs_by_order_by_key=filter_docs_by_order_by_key, doc_id_cursor=doc_id_cursor)

            print(f"Batch load counter {batch_load_request.get_load_batch_counter()} and number of filtered cacher docs {len(filtered_cacher_docs)}")

            # increase bsr range after each loop if not full_batch_size_loaded
            if batch_load_request.is_keyword_search() and len(filtered_cacher_docs) < batch_size:
                bsr_last_range_cursor = batch_load_response.highest_bsr_last_range
                # if a full batch of data was loaded probably more data exists in this bsr_range, except in case new cursor is the same as old one
                if batch_load_response.full_batch_size_loaded and batch_load_request.doc_id_cursor != batch_load_response.last_fs_doc.doc_id:
                    print("full_batch_size_loaded", batch_load_request.doc_id_cursor, batch_load_response.last_fs_doc.doc_id)
                    # if batch size was not completely loaded, we try again with new doc_id_cursor
                    if batch_load_response.last_fs_doc:
                        batch_load_request.doc_id_cursor = batch_load_response.last_fs_doc.doc_id
                # if full batch was loaded but cursors are equal -> we increment bsr range by 1 step
                elif batch_load_response.full_batch_size_loaded and batch_load_request.doc_id_cursor == batch_load_response.last_fs_doc.doc_id:
                    batch_load_request.update_bsr_last_range_filter(1, update_bsr_min_with_bsr_last_range_max=True)
                    print(f"Update bsr_range filter to {filter_simple_query_filters(batch_load_request.simple_query_filters, field=FSMbaShirtsIndexField.BSR_RANGE)}")
                # else take a bigger step depending on loop iteration counter
                else:
                    # bsr_min = batch_load_output.highest_bsr_last_range + 1 if batch_load_output.highest_bsr_last_range != None else None
                    # Problem: Handle case: 4 loops of full_batch_size_loaded than bsr_last_range filter becomes [1,...,9] more than needed. Should increase by one in the gebinning
                    # Solution: if loop_counter is higher than 2 and bsr_range was not updated that much (mutliple full_batch_size_loaded indicator) -> increment bsr_last_range filter by one
                    bsr_last_range_filter_list_length = 1 if batch_load_request.get_load_batch_counter() > 2 and batch_load_request.bsr_last_range_update_counter < 2 else 1 + batch_load_request.get_load_batch_counter() * 3
                    batch_load_request.update_bsr_last_range_filter(bsr_last_range_filter_list_length, update_bsr_min_with_bsr_last_range_max=True)
                    print(f"Update bsr_range filter to {filter_simple_query_filters(batch_load_request.simple_query_filters, field=FSMbaShirtsIndexField.BSR_RANGE)}")
                    #self.update_bsr_range_list_filter(fs_possible_query_filters, None, bsr_range_list_max, min(, 10), update_bsr_min_with_bsr_last_range_max=True)
                    if batch_load_request.stop_load_keyword_search_batches():
                        break

            # break condition to prevent endlees fetching of new data (costs per read)
            if max_load_new_batch_nr <= batch_load_request.get_load_batch_counter():
                # TODO: Try to get data with order by upload_date, if upload filter exissimple_query_filtersts
                #self.load_data_with_order_by_upload_date_with_filters(load_batch_in_cache_size, get_batch_request_id, simple_query_filters=simple_query_filters, doc_id_cursor=doc_id_cursor)
                if not batch_load_request.is_keyword_search():
                    self.load_with_different_order_bys(order_by, load_batch_in_cache_size, batch_load_request.id, simple_query_filters, batch_load_request.doc_id_cursor, bsr_range_list_max, is_keyword_search=batch_load_request.is_keyword_search())
                filter_docs_by_order_by_key = False
                # prevent endless loop and get data independend of order by restriction (filters are applied anyway)
                filtered_cacher_docs: List[CacherDocument] = self.filter_fs_doc_dict(simple_query_filters, batch_size=batch_size,
                                                                order_by=order_by,
                                                                order_by_direction=order_by_direction,
                                                                filter_docs_by_order_by_key=filter_docs_by_order_by_key,doc_id_cursor=doc_id_cursor)
                break
        print(len(self.doc_id2cache_doc.keys()) - number_docs_in_cache_start, "Number of documents added to cache.", "Statistics:", batch_load_request.statistics)
        # append batch_load_request with final statistics data to cache attribute

        if filter_docs_by_order_by_key: # in case wie used only order_by docs order is already right
            return [cacher_doc.fs_document for cacher_doc in filtered_cacher_docs[0:batch_size]]
        else:
            # sort docs first and than get desired batch_size
            if order_by:
                # sort by reference
                filtered_cacher_docs.sort(key=lambda cacher_doc: cacher_doc.fs_document[order_by], reverse=order_by_direction == OrderByDirection.DESC)
            return [cacher_doc.fs_document for cacher_doc in filtered_cacher_docs[0:batch_size]]

    def update_beginning_bsr_last_range_filter(self, batch_load_request, doc_id_cursor, matching_batch_load_requests: list=None, update_only_if_cursor_found=True):
        # speed up meta keyword search requests, by updating bsr_last_range filter with matching_batch_load_requests
        if matching_batch_load_requests:
            bsr_last_range_cursor = max([list(self._uuid2order_by_cursor[cached_batch_load_request.id].values())[0] if cached_batch_load_request.id in self._uuid2order_by_cursor else -1 for cached_batch_load_request in matching_batch_load_requests])
            if bsr_last_range_cursor != None and bsr_last_range_cursor > -1:
                batch_load_request.update_bsr_last_range_filter(1, bsr_last_range_cursor, update_bsr_min_with_bsr_last_range_max=True)
        else:
            # For keyword research beginning bsr_last_range cursor should start with one element and optionally where last cursor ended
            bsr_last_range_cursor = self._order_by_cursors[
                batch_load_request.order_by_key].cursor if batch_load_request.order_by_key in self._order_by_cursors else None
            # if cursor was not found we can try to get it from doc_id_cursor
            # TODO: check if this works after page=1
            # TODO: KeyError: 'B07PW28FZC' fix it. Case doc_id_cursor provided
            if not bsr_last_range_cursor and doc_id_cursor and doc_id_cursor in self.doc_id2cache_doc:
                bsr_last_range_cursor = self.doc_id2cache_doc[doc_id_cursor].fs_document[
                    FSMbaShirtsIndexField.BSR_RANGE]
                print(
                    f"Try to get bsr_last_range_cursor {self.doc_id2cache_doc[doc_id_cursor].fs_document[FSMbaShirtsIndexField.BSR_RANGE]}")
            # only update if it already exists
            if len(filter_simple_query_filters(batch_load_request.simple_query_filters,field=FSMbaShirtsIndexField.BSR_RANGE)) > 0 and (not update_only_if_cursor_found  or bsr_last_range_cursor):
                batch_load_request.update_bsr_last_range_filter(1, bsr_last_range_cursor,
                                                            update_bsr_min_with_bsr_last_range_max=False)

        return bsr_last_range_cursor

    @staticmethod
    def get_bsr_last_range_min_by_filters(simple_query_filters) -> Optional[int]:
        bsr_range_list_filters = filter_simple_query_filters(simple_query_filters,
                                                             field=FSMbaShirtsIndexField.BSR_RANGE)
        return bsr_range_list_filters[0].value[0] if len(bsr_range_list_filters) > 0 and len(bsr_range_list_filters[0].value) > 0 else None


    def get_ordered_cacher_docs(self, order_by: Optional[str], reverse=False) -> List[CacherDocument]:
        ordered_cacher_docs = list(self.doc_id2cache_doc.values())
        # TODO: Can't compare naive and aware datetime.now() <= challenge.datetime_end
        if order_by:
            try:
                ordered_cacher_docs.sort(key=lambda cacher_doc: cacher_doc.fs_document[order_by],
                                  reverse=reverse)
            except Exception as e:
                print("Critical: Could not sort cacher docs by:", order_by, str(e))
        return ordered_cacher_docs

    def get_order_by_cursor(self, doc_id: Optional[str], order_by: Optional[str], reverse=False) -> Optional[Union[float, int, str, datetime, bool]]:
        if doc_id not in self.doc_id2cache_doc or doc_id==None or order_by==None:
            return None
        ordered_cacher_docs = self.get_ordered_cacher_docs(order_by, reverse=reverse)
        return next((x.fs_document[order_by] for x in ordered_cacher_docs if x.doc_id == doc_id), None)