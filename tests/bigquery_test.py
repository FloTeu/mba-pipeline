import unittest
import random
from datetime import date, timedelta
import pandas as pd 
from google.cloud import bigquery

class BQLimitTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bq_table = "test"
        self.bq_dataset = "mba_de"
        self.bq_project_id = "mba-pipeline"
        
        self.bq_rows = 100
        self.sdate = date(1990,3,22)   # start date
        self.edate = date(2021,5,10)  
        self.table_id = "{}.{}.{}".format(self.bq_project_id, self.bq_dataset, self.bq_table)
        self.pandas_table_id = "{}.{}".format(self.bq_dataset, self.bq_table)

    def test_01_create_config(self):
        try:
            plot_x = ",".join([str(d.date()) for d in pd.date_range(self.sdate,self.edate-timedelta(days=1),freq='d').to_list()])
            plot_y = ",".join([str(random.randint(100000,1000000)) for d in range(len(plot_x.split(",")))])
            df = pd.DataFrame({"plot_x": [plot_x for r in range(self.bq_rows)], "plot_y": [plot_y for r in range(self.bq_rows)]})
            df.to_gbq(self.pandas_table_id, if_exists="replace", project_id=self.bq_project_id)
            df_read = pd.read_gbq("SELECT * FROM `{}`".format(self.table_id))
            self.assertTrue(len(df_read.iloc[0]["plot_x"].split(",")) == len(plot_x.split(",")), "Could not save all data in BQ")
            self.assertTrue(len(df_read.iloc[0]["plot_y"].split(",")) == len(plot_y.split(",")), "Could not save all data in BQ")
        except Exception as e:
            print(str(e))

    def test_99_delete_table(self):
        # Construct a BigQuery client object.
        client = bigquery.Client()

        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        client.delete_table(self.table_id, not_found_ok=True)  # Make an API request.
        print("Deleted table '{}'.".format(self.table_id))

if __name__ == '__main__':
    unittest.main()
