import pandas as pd
from google.cloud import bigquery

class BigqueryHandler():
    def __init__(self, marketplace="de", dev=""):
        self.marketplace = marketplace
        self.dev = dev
    
    def get_sql_shirts_detail_daily(self, limit=None, filter=None, until_date=None):
        if limit == None:
            SQL_LIMIT = ""
        elif type(limit) == int and limit > 0:
            SQL_LIMIT = "LIMIT " + str(limit)
        else:
            assert False, "limit is not correctly set"

        SQL_STATEMENT = """
        SELECT t0.asin, t0.price, t0.bsr, CAST(REPLACE(t1.price, ',', '.') as FLOAT64) as price_overview, t0.array_bsr_categorie,
         t0.customer_review_score_mean, t0.customer_review_count, t0.timestamp
        FROM `mba-pipeline.mba_{0}.products_details_daily` t0
        LEFT JOIN (SELECT distinct asin, price FROM `mba-pipeline.mba_{0}.products`) t1 on t1.asin = t0.asin
        order by t0.asin, t0.timestamp desc
        {1}
        """.format(self.marketplace, SQL_LIMIT)
        return SQL_STATEMENT

    def product_details_daily_data2file(self, file_name="products_details_daily.csv"):
        df_shirts_detail_daily = pd.read_gbq(self.get_sql_shirts_detail_daily(), project_id="mba-pipeline", verbose=True).drop_duplicates()
        df_shirts_detail_daily.to_csv(f"{self.marketplace}_{file_name}",index=False)

    def get_product_details_daily_data_by_asin(self, asin_list, file_name="products_details_daily.csv", chunksize=500):
        df_shirts_detail_daily = pd.DataFrame()
        # load the big file in smaller chunks
        for df_chunk in pd.read_csv(f"{self.marketplace}_{file_name}",chunksize=chunksize):
            df_chunk = df_chunk[df_chunk["asin"].isin(asin_list)]
            if df_shirts_detail_daily.empty:
                df_shirts_detail_daily = df_chunk
            else:
                df_shirts_detail_daily = df_shirts_detail_daily.append(df_chunk)
        # dtype change to orignal bq type
        df_shirts_detail_daily['timestamp'] = df_shirts_detail_daily['timestamp'].astype('datetime64[ns]')
        return df_shirts_detail_daily
