

import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import List

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

