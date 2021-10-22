

import logging
import json

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import List
from datetime import date, datetime
import pandas as pd

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

##########################################################################
# Classes

class BQParams():
    def __init__(self,
                 table_id=None,
                 project=None,
                 dataset=None,
                 table=None,
                 location="europe-west3"
                 ):
        ''' A storage class for BigQuery parameters.

        Provide:
            - project, dataset and table or
            - table_id

        Example:
            import mvfunctions.cloud.bigquery as mvbq
            bq_params = mvbq.BQParams(table_id='datalake-263909.labels.pool_labels_relabeled')

        :param project:
        :param dataset:
        :param table:
        '''
        assert table_id or (
            project and dataset and table), "You have to either define the table_id or project, dataset, table"
        if table_id:
            # remove ':'
            table_id = '.'.join(table_id.split(':')) if ':' in table_id else table_id
            project, dataset, table = table_id.split('.')
        else:
            table_id = '.'.join([project, dataset, table])

        self.project = project
        self.dataset = dataset
        self.table = table
        self.table_id = table_id
        self.dataset_id = "{}.{}".format(project, dataset)
        self.destination_table = "{}.{}".format(dataset, table)
        self.table_reference = f"{project}:{dataset}.{table}"
        self.location = location

    def table_query(self, columns: List=None,
                    join_query: str = None,
                    additionals=None,
                    do_sample=None,
                    limit=None,):
        """[summary]

        Args:
            columns (List, optional): [description]. Defaults to None.
            join_query (str, optional): [description]. Defaults to None.
            additionals ([type], optional): [description]. Defaults to None.
            limit ([type], optional): [description]. Defaults to None.
            do_sample ([type], optional): [description]. Defaults to None.

        Returns:
            [type]: [description]
        """
        assert type(columns) == list or columns is None
        if join_query:
            assert join_query[0] == " ", f"join_query need preceeding empty space, has '{join_query[0]}'"
        if columns:
            query = """select {} from {} as t1""".format(", ".join(columns), self.table_id)
        else:
            query = """select * from {} as t1""".format(self.table_id)
        if join_query:
            query += f" {join_query}"
        if do_sample:
            # query += f" TABLESAMPLE SYSTEM (10 PERCENT)"
            # query += f" WHERE RAND() < {}/164656"
            query += f" order by RAND()"
        if additionals:
            query += f" {additionals}"
        if limit:
            query += " limit {}".format(limit)
        return query

def table_exists(bq_params):
    """
    Checks if a bigquery table exists
    :param client:
    :param bq_params:
    :return:
    """
    client = bigquery.Client()
    try:
        client.get_table(bq_params.table_id)  # Make an API request.
        logging.info("Table {} already exists.".format(bq_params.table_id))
        return True
    except NotFound:
        logging.info("Table {} is not found.".format(bq_params.table_id))
        return False

def json_serializable_dumper(obj):
    # makes dict value to json serializable value
    if type(obj) in [datetime, date]:
        return str(obj)
    else:
        return obj

def stream_dict_list2bq(bq_table_id, dict_list, client=None, check_if_table_exists=True):
    """
        Gets a list of dicts which should contain consistent types e.g. datetimes should not be converted to string
        if table exists:
            types will be transformed to json serilizable and streamed in bq table
        else:
            new table will be created
    """
    assert type(dict_list) == list, f"'dict_list' must be of type 'list' but is '{type(dict_list)}'"
    client = client if client else bigquery.Client()
    bq_params = BQParams(bq_table_id)
    # if table not exists it should be created with pandas_gbq
    if check_if_table_exists and not table_exists(bq_params):
        df = pd.DataFrame(dict_list)
        df.to_gbq(bq_params.destination_table, bq_params.project, if_exists="append")
    else: # stream data into BQ
        json_dict_list = [json.loads(json.dumps(dict_i, default=json_serializable_dumper)) for dict_i in dict_list]
        errors = client.insert_rows_json(bq_params.table_id, json_dict_list)  # Make an API request.

