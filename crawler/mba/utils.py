import pandas as pd
from google.cloud import bigquery
import pandas_gbq

client = bigquery.Client()

def get_df_hobbies(language):
    if language == "de":
        df = pandas_gbq.read_gbq("SELECT * FROM keywords.de_hobbies", project_id="mba-pipeline")
    else:
        df = pandas_gbq.read_gbq("SELECT * FROM keywords.en_hobbies", project_id="mba-pipeline")


        
