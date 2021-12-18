import re
from contextlib import suppress
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from mwfunctions.pydantic.base_classes import EnumBase
from mwfunctions.pydantic.firestore.indexes import FSIndexItem, FSIndex
from mwfunctions.time import date_to_integer
from pydantic import BaseModel, Field


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


# custom MW

def do_index_items_contain_simple_query_filter(simple_query_filters: FSSimpleFilterQuery, fs_index_items: List[FSIndexItem]) -> bool:
    # TODO make True only if index_item.index_option matches simple_query_filters.comparison_operator
    return any([index_item.field == simple_query_filters.field for index_item in fs_index_items])


class OrderByDirection(str, Enum):
    ASC="asc"
    DESC="desc"


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