import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import itertools
from sklearn import preprocessing
import os 
from os.path import join
from datetime import date
import re
import datetime 
import time
from plotly.offline import plot
import plotly.graph_objs as go
from plotly.graph_objs import Scatter 
from plotly.graph_objs import Layout 
import gc
from django.conf import settings
import logging

class DataHandler():
    def __init__(self):
        self.filePath = None
        self.df_shirts_detail_daily = None

    def get_sql_shirts(self, marketplace, limit=None, filter=None):
        if limit == None:
            SQL_LIMIT = ""
        elif type(limit) == int and limit > 0:
            SQL_LIMIT = "LIMIT " + str(limit)
        else:
            assert False, "limit is not correctly set"

        
        if filter == None:
            SQL_WHERE= "where bsr != 0 and bsr != 404"
        elif filter == "only 404":
            SQL_WHERE = "where bsr = 404"
        elif filter == "only 0":
            SQL_WHERE = "where bsr = 0"
        else:
            assert False, "filter is not correctly set"

        SQL_STATEMENT = """
        SELECT t_fin.* FROM (
    SELECT t0.*, t2.title, t2.brand, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t1.url_mba_hq, t1.url_mba_lowq FROM (
        SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
                AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min, COUNT(*) as bsr_count,
                AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
                FROM `mba-pipeline.mba_{0}.products_details_daily`
        where bsr != 0 and bsr != 404
        group by asin
        ) t0
        left join `mba-pipeline.mba_de.products_images` t1 on t0.asin = t1.asin
        left join `mba-pipeline.mba_de.products_details` t2 on t0.asin = t2.asin

        union all 
        
        SELECT t0.*, t2.title, t2.brand, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t1.url_mba_hq, t1.url_mba_lowq FROM (
        SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
                AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min, COUNT(*) as bsr_count,
                AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
                FROM `mba-pipeline.mba_{0}.products_details_daily`
        where bsr = 0 and bsr != 404
        and asin NOT IN (SELECT asin FROM `mba-pipeline.mba_de.products_details_daily` WHERE bsr != 0 and bsr != 404 group by asin)
        group by asin
        ) t0
        left join `mba-pipeline.mba_de.products_images` t1 on t0.asin = t1.asin
        left join `mba-pipeline.mba_de.products_details` t2 on t0.asin = t2.asin
        
        ) t_fin
        order by t_fin.bsr_mean
        {1}
        """.format(marketplace, SQL_LIMIT)
        return SQL_STATEMENT

    def get_sql_shirts_detail_daily(self, marketplace, asin_list=[], limit=None, filter=None):
        SQL_WHERE_IN = "('" + "','".join(asin_list) + "')"
        if limit == None:
            SQL_LIMIT = ""
        elif type(limit) == int and limit > 0:
            SQL_LIMIT = "LIMIT " + str(limit)
        else:
            assert False, "limit is not correctly set"

        SQL_STATEMENT = """
        SELECT asin, price, bsr, price_str, bsr_str, timestamp FROM `mba-pipeline.mba_{0}.products_details_daily`
        where asin in {1}
        order by asin, timestamp desc
        {2}
        """.format(marketplace, SQL_WHERE_IN, SQL_LIMIT)
        return SQL_STATEMENT

    def make_trend_column(self, df_shirts):
        df_shirts = df_shirts.reset_index(drop=True)
        x = df_shirts[["time_since_upload"]].values 
        min_max_scaler = preprocessing.MinMaxScaler()
        x_scaled = min_max_scaler.fit_transform(x)
        df = pd.DataFrame(x_scaled)
        df_shirts["time_since_upload_norm"] = df.iloc[:,0] + 0.001
        df_shirts.loc[(df_shirts['bsr_last'] == 0.0), "bsr_last"] = 999999999
        df_shirts.loc[(df_shirts['bsr_last'] == 404.0), "bsr_last"] = 999999999
        df_shirts["trend"] = df_shirts["bsr_last"] * df_shirts["time_since_upload_norm"] * 2
        df_shirts = df_shirts.sort_values("trend", ignore_index=True).reset_index(drop=True)
        df_shirts["trend_nr"] = df_shirts.index + 1
        return df_shirts

    def update_bq_shirt_tables(self, marketplace, chunk_size=500, limit=None, filter=None):
        # This part should only triggered once a day to update all relevant data
        print("Load shirt data from bigquery")
        start_time = time.time()
        project_id = 'mba-pipeline'
        bq_client = bigquery.Client(project=project_id)
        df_shirts = pd.read_gbq(self.get_sql_shirts(marketplace, None, None), project_id="mba-pipeline").drop_duplicates()
        #df_shirts = bq_client.query(self.get_sql_shirts(marketplace, None, None)).to_dataframe().drop_duplicates()
        # This dataframe is expanded with additional information with every chunk 
        df_shirts_with_more_info = df_shirts.copy()

        chunk_size = chunk_size  #chunk row size
        print("Chunk size: "+ str(chunk_size))
        df_shirts_asin = df_shirts[["asin"]].copy()

        df_shirts_asin_chunks = [df_shirts_asin[i:i+chunk_size] for i in range(0,df_shirts_asin.shape[0],chunk_size)]
        for i, df_shirts_asin_chunk in enumerate(df_shirts_asin_chunks):
            print("Chunk %s of %s" %(i, len(df_shirts_asin_chunks)))
            asin_list = df_shirts_asin_chunk["asin"].tolist()
            if_exists = "append"
            if i == 0:
                if_exists="replace"
            print("Start to get chunk from bigquery")
            try:
                self.df_shirts_detail_daily = pd.read_gbq(self.get_sql_shirts_detail_daily(marketplace,asin_list=asin_list, limit=limit), project_id="mba-pipeline", verbose=True).drop_duplicates()
                #self.df_shirts_detail_daily = bq_client.query(self.get_sql_shirts_detail_daily(marketplace,asin_list=asin_list, limit=limit)).to_dataframe().drop_duplicates()
            #df_shirts_detail_daily["date"] = df_shirts_detail_daily.apply(lambda x: datetime.datetime.strptime(re.search(r'\d{4}-\d{2}-\d{2}', x["timestamp"]).group(), '%Y-%m-%d').date(), axis=1)
            except Exception as e:
                print(str(e))
                raise ValueError
            print("Got bigquery chunk")
            self.df_shirts_detail_daily["date"] = self.df_shirts_detail_daily.apply(lambda x: x["timestamp"].date(), axis=1)
         
            print("Start to get first and last bsr of shirts")
            df_additional_data = df_shirts_asin_chunk.apply(lambda x: pd.Series(self.get_first_and_last_data(x["asin"])), axis=1)
            df_additional_data.columns=["bsr_last", "price_last", "bsr_first", "price_first", "bsr_change", "price_change"]
            df_shirts_with_more_info_append = df_shirts.merge(df_additional_data, 
                left_index=True, right_index=True)
            if i == 0:
                df_shirts_with_more_info = df_shirts_with_more_info_append
            else:
                df_shirts_with_more_info = df_shirts_with_more_info.append(df_shirts_with_more_info_append)

            print("Start to create plots")
            df_shirts_asin_chunk = df_shirts_asin_chunk.merge(df_additional_data, 
                left_index=True, right_index=True)
            df_shirts_asin_chunk["plot"] = df_shirts_asin_chunk.apply(lambda x: self.create_plot_html(x), axis=1)
            df_shirts_asin_chunk.to_gbq("mba_" + str(marketplace) +".plots", project_id="mba-pipeline", if_exists=if_exists)
            gc.collect()
        
        df_shirts_with_more_info = self.make_trend_column(df_shirts_with_more_info)
        # try to calculate trend change
        try:
            df_shirts_old=pd.read_gbq("SELECT * FROM mba_" + str(marketplace) +".merchwatch_shirts", project_id="mba-pipeline")
            df_shirts_old["trend_nr_old"] = df_shirts_old["trend_nr"]
            # transform older trend nr (yesterday) in same dimension as new trend nr
            df_shirts_with_more_info = df_shirts_with_more_info.merge(df_shirts_old[["asin", "trend_nr_old"]],how='left', on='asin')
            df_shirts_with_more_info[['trend_nr_old']] = df_shirts_with_more_info[['trend_nr_old']].fillna(value=0)
            
            df_shirts_with_more_info["trend_change"] = df_shirts_with_more_info.apply(lambda x: 0 if int(x["trend_nr_old"]) == 0 else int(x["trend_nr_old"] - x["trend_nr"]),axis=1)
        except:
            df_shirts_with_more_info["trend_change"] = 0
        # save dataframe with shirts in local storage
        df_shirts_with_more_info.to_gbq("mba_" + str(marketplace) +".merchwatch_shirts", project_id="mba-pipeline", if_exists="replace")
        # make memory space free
        self.df_shirts_detail_daily = None
        print("Loading completed. Elapsed time: %.2f minutes" %((time.time() - start_time) / 60))

    def get_change(self, current, previous):
        current = float(current)
        previous = float(previous)
        if current == previous:
            return 0
        try:
            return ((current - previous) / previous) * 100.0
        except ZeroDivisionError:
            return 0

    def get_first_and_last_data(self, asin):
        # return last_bsr, last_price, first_bsr, first_price
        occurences = (self.df_shirts_detail_daily.asin.values == asin)
        if len(self.df_shirts_detail_daily[occurences]) == 0:
            return 0,0,0,0,0,0
        else:
            last_occ = self.df_shirts_detail_daily[occurences].iloc[0]
        first_occ = self.df_shirts_detail_daily[occurences].iloc[-1]
        return last_occ["bsr"], last_occ["price"], first_occ["bsr"], first_occ["price"], self.get_change(last_occ["bsr"], first_occ["bsr"]), self.get_change(last_occ["price"], first_occ["price"])

    def create_plot_html(self, df_shirts_row):
        config = {'displayModeBar': False, 'responsive': True}#{"staticPlot": True}
        df_asin_detail_daily = self.df_shirts_detail_daily[self.df_shirts_detail_daily["asin"]==df_shirts_row["asin"]]
        marker_color = "black"
        if df_shirts_row["bsr_change"] > 0:
            marker_color = "red"
        elif df_shirts_row["bsr_change"] < 0:
            marker_color = "green"

        #plot_div = plot([Scatter(x=df_asin_detail_daily["date"].tolist(), y=df_asin_detail_daily["bsr"].tolist(),
        #                mode='lines', name='plot_' + df_shirts_row["asin"],
        #                opacity=0.8, marker_color='green', showlegend=False, yaxis="y"
        #                )
        #        ],
        #        output_type='div', include_plotlyjs=False, show_link=False, link_text="",image_width=400, image_height=300, config=config)

        plot_div = plot({"data": [go.Scatter(x=df_asin_detail_daily["date"].tolist(), y=df_asin_detail_daily["bsr"].tolist(),
                        mode='lines', name='plot_' + df_shirts_row["asin"],
                        opacity=0.8, marker_color=marker_color, showlegend=False, yaxis="y")],
                     "layout": go.Layout(yaxis = dict(visible=True, autorange="reversed"), autosize=True, margin={'t': 0,'b': 0,'r': 0,'l': 0} )},
                output_type='div', include_plotlyjs=False, show_link=False, link_text="", config=config)
        return plot_div

