import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import datastore
import itertools
from sklearn import preprocessing
import os 
from os.path import join
from datetime import date, datetime, timedelta
import re
import time
from plotly.offline import plot
import plotly.graph_objs as go
from plotly.graph_objs import Scatter 
from plotly.graph_objs import Layout 
import gc
from django.conf import settings
import logging
from firestore_handler import Firestore
from text_rank import TextRank4Keyword
from langdetect import detect

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
    SELECT t0.*, t2.title, t2.brand, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t1.url_mba_hq, t1.url_mba_lowq, t3.url_image_q2, t3.url_image_q3, t3.url_image_q4 FROM (
        SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
                AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min, COUNT(*) as bsr_count,
                AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
                FROM `mba-pipeline.mba_{0}.products_details_daily`
        where bsr != 0 and bsr != 404
        group by asin
        ) t0
        left join `mba-pipeline.mba_{0}.products_images` t1 on t0.asin = t1.asin
        left join `mba-pipeline.mba_{0}.products_details` t2 on t0.asin = t2.asin
        left join `mba-pipeline.mba_{0}.products_mba_images` t3 on t0.asin = t3.asin

        union all 
        
        SELECT t0.*, t2.title, t2.brand, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t1.url_mba_hq, t1.url_mba_lowq, t3.url_image_q2, t3.url_image_q3, t3.url_image_q4 FROM (
        SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
                AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min, COUNT(*) as bsr_count,
                AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
                FROM `mba-pipeline.mba_{0}.products_details_daily`
        where bsr = 0 and bsr != 404
        and asin NOT IN (SELECT asin FROM `mba-pipeline.mba_{0}.products_details_daily` WHERE bsr != 0 and bsr != 404 group by asin)
        group by asin
        ) t0
        left join `mba-pipeline.mba_{0}.products_images` t1 on t0.asin = t1.asin
        left join `mba-pipeline.mba_{0}.products_details` t2 on t0.asin = t2.asin
        left join `mba-pipeline.mba_{0}.products_mba_images` t3 on t0.asin = t3.asin
        
        ) t_fin
        order by t_fin.bsr_mean
        {1}
        """.format(marketplace, SQL_LIMIT)
        return SQL_STATEMENT

    def get_sql_shirts_detail_daily(self, marketplace, asin_list=[], limit=None, filter=None, until_date=None):
        SQL_WHERE_IN = "('" + "','".join(asin_list) + "')"
        if limit == None:
            SQL_LIMIT = ""
        elif type(limit) == int and limit > 0:
            SQL_LIMIT = "LIMIT " + str(limit)
        else:
            assert False, "limit is not correctly set"
        until_time = ""
        if until_date != None:
            until_time = "and timestamp <= '%s'" % until_date

        SQL_STATEMENT = """
        SELECT t0.asin, t0.price, t0.bsr, CAST(REPLACE(t1.price, ',', '.') as FLOAT64) as price_overview, t0.timestamp
        FROM `mba-pipeline.mba_{0}.products_details_daily` t0
        LEFT JOIN (SELECT distinct asin, price FROM `mba-pipeline.mba_{0}.products`) t1 on t1.asin = t0.asin
        where t0.asin in {1} {3}
        order by t0.asin, t0.timestamp desc
        {2}
        """.format(marketplace, SQL_WHERE_IN, SQL_LIMIT, until_time)
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

        
    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        # bucket_name = "your-bucket-name"
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client(project='mba-pipeline')
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        '''
        print(
            "File {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )
        '''

    def create_folder_if_not_exists(self, folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def upload_plot_data(self,df,marketplace,dev):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        self.create_folder_if_not_exists("data")
        for i, df_row in df.iterrows():
            asin = df_row["asin"]
            # save product information string locally
            with open(join("data",asin+".html"), "w") as f:
                f.write(df_row["plot"])
            # store plots in storage
            self.upload_blob("merchwatch-de-media", join("data",asin+".html"), join("plots" + dev_str,marketplace, asin + ".html"))
    
    def replace_price_last_zero(self, marketplace):
        project_id = 'mba-pipeline'
        bq_client = bigquery.Client(project=project_id)

        SQL_STATEMENT = '''CREATE OR REPLACE TABLE `mba-pipeline.mba_{0}.merchwatch_shirts`
        AS        
        SELECT CASE WHEN t0.price_last = 0.0 THEN  CAST(REPLACE(
            t1.price,
            ',',
            '.') as FLOAT64) ELSE t0.price_last END as price_last
        , t0.* EXCEPT(price_last) FROM `mba-pipeline.mba_{0}.merchwatch_shirts` t0 
        LEFT JOIN (SELECT distinct asin, price FROM `mba-pipeline.mba_{0}.products`) t1 on t1.asin = t0.asin
        '''.format(marketplace)

        query_job = bq_client.query(SQL_STATEMENT)
        query_job.result()

    def update_bq_shirt_tables(self, marketplace, chunk_size=500, limit=None, filter=None,dev=False):
        # This part should only triggered once a day to update all relevant data
        print("Load shirt data from bigquery")
        start_time = time.time()
        project_id = 'mba-pipeline'
        bq_client = bigquery.Client(project=project_id)
        df_shirts = pd.read_gbq(self.get_sql_shirts(marketplace, None, None), project_id="mba-pipeline").drop_duplicates(["asin"])
        #df_shirts = bq_client.query(self.get_sql_shirts(marketplace, None, None)).to_dataframe().drop_duplicates()
        # This dataframe is expanded with additional information with every chunk 
        df_shirts_with_more_info = df_shirts.copy()

        chunk_size = chunk_size  #chunk row size
        print("Chunk size: "+ str(chunk_size))
        df_shirts_asin = df_shirts[["asin"]].copy()

        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

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

            # get plot data
            print("Start to get plot data of shirts")
            start_time = time.time()
            plot_data = df_shirts_asin_chunk.apply(lambda x: self.create_plot_data(x), axis=1)
            plot_x=[]
            plot_y=[]
            for plot_data_i in plot_data:
                plot_x.append(plot_data_i[0])
                plot_y.append(plot_data_i[1])
            print("elapsed time: %.2f sec" %((time.time() - start_time)))
 
            print("Start to get first and last bsr of shirts")
            start_time = time.time()
            df_additional_data = df_shirts_asin_chunk.apply(lambda x: pd.Series(self.get_first_and_last_data(x["asin"])), axis=1)
            df_additional_data.columns=["bsr_last", "price_last", "bsr_first", "price_first", "bsr_change", "bsr_change_total", "price_change", "update_last"]
            df_additional_data["plot_x"],df_additional_data["plot_y"] = plot_x, plot_y

            df_shirts_with_more_info_append = df_shirts.merge(df_additional_data, 
                left_index=True, right_index=True)
            if i == 0:
                df_shirts_with_more_info = df_shirts_with_more_info_append
            else:
                df_shirts_with_more_info = df_shirts_with_more_info.append(df_shirts_with_more_info_append)
            print("elapsed time: %.2f sec" %((time.time() - start_time)))

            '''
            print("Start to create plot html")
            start_time = time.time()
            df_shirts_asin_chunk = df_shirts_asin_chunk.merge(df_additional_data, 
                left_index=True, right_index=True)
            df_shirts_asin_chunk["plot"] = df_shirts_asin_chunk.apply(lambda x: self.create_plot_html(x), axis=1)
            self.upload_plot_data(df_shirts_asin_chunk,marketplace,dev)
            print("elapsed time: %.2f min" %((time.time() - start_time)/60))
            #df_shirts_asin_chunk.to_gbq("mba_" + str(marketplace) +".plots" + dev_str, project_id="mba-pipeline", if_exists=if_exists)
            '''
            gc.collect()
        
        df_shirts_with_more_info = self.make_trend_column(df_shirts_with_more_info)

        # try to calculate trend change
        df_shirts_old=pd.read_gbq("SELECT DISTINCT asin, trend_nr, bsr_last FROM mba_" + str(marketplace) +".merchwatch_shirts" + dev_str, project_id="mba-pipeline")
        df_shirts_old["trend_nr_old"] = df_shirts_old["trend_nr"].astype(int)
        df_shirts_old["bsr_last_old"] = df_shirts_old["bsr_last"].astype(int)
        # transform older trend nr (yesterday) in same dimension as new trend nr
        df_shirts_with_more_info = df_shirts_with_more_info.merge(df_shirts_old[["asin", "trend_nr_old", "bsr_last_old"]],how='left', on='asin')
        try:

            df_shirts_with_more_info['trend_nr_old'] = df_shirts_with_more_info['trend_nr_old'].fillna(value=0).astype(int)
            
            df_shirts_with_more_info["trend_change"] = df_shirts_with_more_info.apply(lambda x: 0 if int(x["trend_nr_old"]) == 0 else int(x["trend_nr_old"] - x["trend_nr"]),axis=1)
        except Exception as e:
            df_shirts_with_more_info["trend_change"] = 0
        # try to create has_bsr_last_changed column
        try:
            df_shirts_with_more_info['bsr_last_old'] = df_shirts_with_more_info['bsr_last_old'].fillna(value=0).astype(int)
            df_shirts_with_more_info['bsr_last_change'] = (df_shirts_with_more_info["bsr_last_old"] - df_shirts_with_more_info["bsr_last"]).astype(int)
            # get the date one week ago
            date_one_week_ago = (datetime.now() - timedelta(days = 7)).date()
            # filter df which should always be updated (update newer than 7 days + bsr_count equals 1 or 2 or trend_nr lower or equal to 2000) 
            df_should_update = df_shirts_with_more_info[((df_shirts_with_more_info["bsr_count"]<=2) & (df_shirts_with_more_info["update_last"]>=date_one_week_ago)) | (df_shirts_with_more_info["trend_nr"]<=2000)]
            # change bsr_last_change to 1 for those how should be updated independent of bsr_last
            df_shirts_with_more_info.loc[df_should_update.index, "bsr_last_change"] = 1
            df_shirts_with_more_info['has_bsr_last_changed'] = df_shirts_with_more_info['bsr_last_change'] != 0
        except Exception as e:
            df_shirts_with_more_info["has_bsr_last_changed"] = True

        # save dataframe with shirts in local storage
        print("Length of dataframe", len(df_shirts_with_more_info),dev_str)
        df_shirts_with_more_info.to_gbq("mba_" + str(marketplace) +".merchwatch_shirts" + dev_str, project_id="mba-pipeline", if_exists="replace")
        self.replace_price_last_zero(marketplace)
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
    
    def get_change_total(self, current, previous):
        current = float(current)
        previous = float(previous)
        if current == previous:
            return 0
        try:
            return current - previous
        except ZeroDivisionError:
            return 0

    def get_first_and_last_data(self, asin, with_asin=False):
        # return last_bsr, last_price, first_bsr, first_price
        occurences = (self.df_shirts_detail_daily.asin.values == asin)
        df_occ = self.df_shirts_detail_daily[occurences]
        if len(df_occ) == 0:
            return 0,0,0,0,0,0,0,0
        else:
            i = 0
            # try to get last bsr which is unequal to zero. If only zero bsr exists return last occurence
            while True:
                try:
                    last_occ = df_occ.iloc[i]
                except:
                    last_occ = df_occ.iloc[0]
                    break
                if int(last_occ["bsr"]) != 0:
                    break
                i += 1
            i = 0
            # try to get last price which is unequal to zero. If only zero bsr exists return last occurence
            while True:
                try:
                    last_occ_price = df_occ.iloc[i]
                except:
                    last_occ_price = df_occ.iloc[0]
                    break
                if int(last_occ_price["price"]) != 0.0:
                    break
                i += 1
            i = 1
            # try to get first bsr which is unequal to zero. If only zero bsr exists return first occurence
            while True:
                try:
                    first_occ_ue_zero = df_occ.iloc[-i]
                except:
                    first_occ_ue_zero = df_occ.iloc[-1]
                    break
                if int(first_occ_ue_zero["bsr"]) != 0:
                    break
                i += 1
            i = 1
            # try to get first price which is unequal to zero. If only zero bsr exists return first occurence
            while True:
                try:
                    first_occ_price_ue_zero = df_occ.iloc[-i]
                except:
                    first_occ_price_ue_zero = df_occ.iloc[-1]
                    break
                if int(first_occ_price_ue_zero["price"]) != 0.0:
                    break
                i += 1
        # get first occurence of data
        first_occ = df_occ.iloc[-1]

        # try to first occurence 4 weeks in the past 
        # if not possible use the first occurence of bsr un equal to zero
        last_n_weeks = 4
        days = 30
        date_N_weeks_ago = datetime.now() - timedelta(days=days)
        try:
            occ_4w = df_occ[df_occ['date'] <= date_N_weeks_ago.date()]
            if len(occ_4w) == 0:
                occ_4w = first_occ_ue_zero
            # make sure that occ_4w contains an value unequal to zero if existent
            elif occ_4w.iloc[0]["bsr"] == 0:
                occ_4w = first_occ_ue_zero
            else:
                occ_4w = occ_4w.iloc[0]
        except:
            occ_4w = first_occ_ue_zero

        if last_occ_price["price"] == 0:
            try:
                price_last = df_occ.iloc[0]["price_overview"]
            except:
                price_last = last_occ_price["price"]
        else:
            price_last = last_occ_price["price"]

        if with_asin:
            return last_occ["bsr"], price_last, first_occ["bsr"], first_occ_price_ue_zero["price"], self.get_change_total(last_occ["bsr"], occ_4w["bsr"]), self.get_change_total(last_occ["bsr"], first_occ["bsr"]), self.get_change_total(last_occ["price"], first_occ["price"]), last_occ["date"], asin
        else:
            return last_occ["bsr"], price_last, first_occ["bsr"], first_occ_price_ue_zero["price"], self.get_change_total(last_occ["bsr"], occ_4w["bsr"]), self.get_change_total(last_occ["bsr"], first_occ["bsr"]), self.get_change_total(last_occ["price"], first_occ["price"]), last_occ["date"]

    def create_plot_html(self, df_shirts_row):
        config = {'displayModeBar': False, 'responsive': True}#{"staticPlot": True}
        df_asin_detail_daily = self.df_shirts_detail_daily[self.df_shirts_detail_daily["asin"]==df_shirts_row["asin"]]
        # remove bsr with 0 
        df_asin_detail_daily = df_asin_detail_daily[df_asin_detail_daily["bsr"]!=0]
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

    def create_plot_data(self, df_shirts_row):
        config = {'displayModeBar': False, 'responsive': True}#{"staticPlot": True}
        df_asin_detail_daily = self.df_shirts_detail_daily[self.df_shirts_detail_daily["asin"]==df_shirts_row["asin"]]
        # remove bsr with 0 
        df_asin_detail_daily = df_asin_detail_daily[df_asin_detail_daily["bsr"]!=0]
        x=",".join(x.strftime("%d/%m/%Y") for x in df_asin_detail_daily["date"].tolist())
        y=",".join(str(y) for y in df_asin_detail_daily["bsr"].tolist())
        return x, y

    def insert_df_to_datastore(self, df, kind):
        dclient = datastore.Client()
        # The kind for the new entity
        columns = df.columns.values
        entities = []
        row_count = len(df)
        for i, row in df.iterrows():
            if i % 1000 == 0:
                print("row {} of {}".format(i, row_count))
            modulo = ((i+1) % 500)
            if modulo != 0:
                # The Cloud Datastore key for the new entity
                task_key = dclient.key(kind, row["asin"])
                # Prepares the new entity
                entity = datastore.Entity(key=task_key)

                for column in columns:
                    if column != "plot":
                        entity[column] = row[column]
                entities.append(entity)
            else:
                # Saves the entity
                try:
                    if i != 0 and len(entities) > 0: 
                        dclient.put_multi(entities)
                except Exception as e:
                    print(str(e))
                    raise e
                entities = []


    def get_shirt_dataset_sql(self, marketplace, dev=False, update_all=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        ORDERBY_STATEMENT = "order by trend_nr"
        WHERE_STATEMENT = ""
        if not update_all:
            WHERE_STATEMENT = "where t_fin.has_bsr_last_changed"
        SQL_STATEMENT = """
        SELECT t_fin.* FROM (
            SELECT t_tmp.*, ROW_NUMBER() OVER() row_number FROM (
                SELECT t0.*,t2.url_affiliate,t2.img_affiliate, CASE WHEN t2.img_affiliate IS NOT NULL THEN true ELSE false END as affiliate_exists FROM 
                -- remove duplicates by choosing only the first entry of asin
                (
                SELECT ARRAY_AGG(t LIMIT 1)[OFFSET(0)] t0
                    FROM `mba-pipeline.mba_{0}.merchwatch_shirts{2}` t
                    GROUP BY asin
                    )   
            left join 
            (
            -- remove duplicates by choosing only the first entry of asin
            SELECT ARRAY_AGG(t_aff LIMIT 1)[OFFSET(0)] t2
                FROM `mba-pipeline.mba_{0}.products_affiliate_urls` t_aff
                GROUP BY asin
                )  on t0.asin = t2.asin 
                
            WHERE t0.bsr_mean != 0 and t0.price_last != 404 and t0.upload_date IS NOT NULL
             {1}
            ) t_tmp
        ) t_fin {3}
        
        """.format(marketplace, ORDERBY_STATEMENT, dev_str, WHERE_STATEMENT)

        return SQL_STATEMENT

    def get_shirt_dataset(self, marketplace, dev=False, update_all=False):
        shirt_sql = self.get_shirt_dataset_sql(marketplace, dev=dev, update_all=update_all)
        try:
            df_shirts=pd.read_gbq(shirt_sql, project_id="mba-pipeline")
        except Exception as e:
            print(str(e))
            raise e
        return df_shirts

    def update_datastore(self, marketplace, kind, dev=False, update_all=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        df = self.get_shirt_dataset(marketplace, dev=dev, update_all=update_all)
        self.insert_df_to_datastore(df, kind + dev_str)
        df = self.get_shirt_dataset_404(marketplace, dev=dev)
        self.delete_list_asin_from_datastore(marketplace, df["asin"].drop_duplicates().tolist(), dev=dev)

    def get_shirt_dataset_404_sql(self, marketplace, dev=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        SQL_STATEMENT = """
        SELECT DISTINCT asin FROM `mba-pipeline.mba_{0}.merchwatch_shirts{1}` where price_last = 404
        
        """.format(marketplace, dev_str)

        return SQL_STATEMENT

    def get_shirt_dataset_404(self, marketplace, dev=False):
        shirt_sql = self.get_shirt_dataset_404_sql(marketplace, dev=dev)
        try:
            df_shirts=pd.read_gbq(shirt_sql, project_id="mba-pipeline")
        except Exception as e:
            print(str(e))
            raise e
        return df_shirts

    def delete_list_asin_from_datastore(self, marketplace, list_asin, dev=False):
        """
            Remove all given asins from datastore
        """
        dclient = datastore.Client()
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"
        kind = marketplace + "_shirts" + dev_str
        list_keys = []
        list_keys_i = []
        for i, asin in enumerate(list_asin):
            if (i+1) % 500 == 0:
                list_keys.append(list_keys_i)
                list_keys_i = []
            list_keys_i.append(datastore.key.Key(kind, asin, project="mba-pipeline"))
            print("Delete key with asin: " + str(asin))
        list_keys.append(list_keys_i)
        for list_keys_i in list_keys:
            dclient.delete_multi(list_keys_i)

    def update_firestore(self, marketplace, collection, dev=False, update_all=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        df = self.get_shirt_dataset(marketplace, dev=dev, update_all=update_all)

        def create_keywords(df_row):
            asin = df_row["asin"].lower()
            brand_list = df_row["brand"].lower().split(" ")
            title_list = df_row["title"].lower().split(" ")
            # get only first two feature bullets (listings)
            product_features_list = [feature.strip().replace("'","") for feature in df_row["product_features"].strip("[]").split("',")][0:2]
            product_features_keywords_list = []
            for product_features in product_features_list:
                words = re.findall(r'\w+', product_features) 
                product_features_keywords_list.extend([v.lower() for v in words])


            keywords = [asin] + brand_list + title_list + product_features_keywords_list
            keywords_2word = []
            for i in range(len(keywords)):
                keywords_2word.append(" ".join(keywords[i:i+2]))
            keywords_3word = []
            for i in range(len(keywords)):
                keywords_3word.append(" ".join(keywords[i:i+3]))

            return keywords + keywords_2word[0:-1] + keywords_3word[0:-2]

        df["keywords"] = df.apply(lambda x: create_keywords(x), axis=1)
        firestore = Firestore(collection + dev_str)
        firestore.update_by_df_batch(df, "asin")


    def count_slashes(self, string):
        count = 0
        string_list = string.split("\'")
        return len(string_list)

    def increment_count_list(self, count_list, index):
        try:
            count_list[index] = count_list[index] + 1
        except:
            count_list.append(1)
        return count_list

    def set_zero_in_count_list_if_not_existend(self, count_list, index):
        try:
            value = count_list[index]
        except:
            count_list.append(0)
        return count_list

    def get_mean_and_variance(self, digit_list, return_integer=True):
        if len(digit_list) == 0:
            return 0,0
        elif type(digit_list[0]) not in [int,float]:
            raise ValueError("Elements in list element must be of type int or float")
        else:
            mean = sum(digit_list)/len(digit_list)
            variance = np.var(digit_list)
            if return_integer:
                mean = int(mean)
                variance = int(variance)
            else:
                # float with only two digits
                mean = float("{:.2f}".format(mean))
                variance = float("{:.2f}".format(variance))
            return mean, variance

    def keyword_dicts_to_df(self, keywords_asin, keywords_count, keywords_bsr_last, keywords_trend, keywords_price_last, keywords_bsr_change):
        # setup columns for dataframe
        keyword_list = []
        count_list = []
        count_with_bsr_list = []
        count_without_bsr_list = []
        count_with_404_list = []
        bsr_mean_list = []
        bsr_change_mean_list = []
        bsr_change_variance_list = []
        bsr_variance_list = []
        price_mean_list = []
        price_variance_list = []
        trend_mean_list = []
        trend_variance_list = []
        asin_list = []

        # iterate over keywords
        for index, keyword in enumerate(list(keywords_count.keys())):
            # filter bsr_last which is to high -> equal to None or 404
            bsr_last_list = keywords_bsr_last[keyword]
            price_last_list = keywords_price_last[keyword]
            trend_list_filtered = keywords_trend[keyword]
            bsr_list_filtered = []
            price_list_filtered = []
            count_with_bsr = 0
            count_without_bsr = 0
            count_404 = 0 
            for i, bsr_last in enumerate(bsr_last_list):
                price_last = price_last_list[i]
                trend = trend_list_filtered[i]
                # increase count of with_bsr without_bsr and 404 
                if bsr_last == 0:
                    count_without_bsr = count_without_bsr + 1
                    price_list_filtered.append(price_last)
                elif bsr_last == 404 and price_last == 404:
                    count_404 = count_404 + 1   
                # only if bsr value exists update bsr and trend list             
                else:
                    count_with_bsr = count_with_bsr + 1   
                    bsr_list_filtered.append(bsr_last)
                    price_list_filtered.append(price_last)
                    trend_list_filtered.append(trend)

            if len(bsr_list_filtered) > 0:
                bsr_last_mean, bsr_last_variance = self.get_mean_and_variance(bsr_list_filtered)
            else:
                # ignore this keyword if no bsr exists
                continue
            
            # fill count lists of with_bsr without_bsr and 404 
            count_without_bsr_list.append(count_without_bsr)
            count_with_404_list.append(count_404)
            count_with_bsr_list.append(count_with_bsr)

            # fill bsr_change list 
            bsr_change_list = [v for v in keywords_bsr_change[keyword] if v != 0]
            bsr_change_mean, bsr_change_variance = self.get_mean_and_variance(bsr_change_list)
            bsr_change_mean_list.append(bsr_change_mean)
            bsr_change_variance_list.append(bsr_change_variance)

            # fill bsr lists
            bsr_mean_list.append(bsr_last_mean)
            bsr_variance_list.append(bsr_last_variance)
            
            # fill price lists
            price_mean, price_variance = self.get_mean_and_variance(price_list_filtered, return_integer=False)
            price_mean_list.append(price_mean)
            price_variance_list.append(price_variance)

            # fill general keyword lists
            keyword_list.append(keyword)
            keyword_count = keywords_count[keyword]
            count_list.append(keyword_count) 

            # fill asin lists
            if len(keywords_asin[keyword]) != len(set(keywords_asin[keyword])):
                print("Duplicates found for asins and keyword %s" % keyword)
                raise ValueError("Duplicates found for asins and keyword %s" % keyword)

            asin_list.append(",".join(keywords_asin[keyword]))

            # fill trend lists
            trend_mean, trend_variance = self.get_mean_and_variance(trend_list_filtered)
            trend_mean_list.append(trend_mean)
            trend_variance_list.append(trend_variance)
        
        # return final niche dataframe
        return pd.DataFrame({"keyword":keyword_list, "count": count_list, "count_with_bsr": count_with_bsr_list, "count_without_bsr": count_without_bsr_list, "count_404": count_with_404_list,
         "bsr_mean": bsr_mean_list, "bsr_variance": bsr_variance_list, "trend_mean": trend_mean_list, "trend_variance": trend_variance_list, "bsr_change_mean": bsr_change_mean_list,
         "bsr_change_variance":bsr_change_variance_list, "price_mean": price_mean_list, "price_variance": price_variance_list, "asin": asin_list})
    
    def filter_keywords(self, keywords, keywords_to_remove, single_words_to_filter=["t","du"]):
        keywords_filtered = []
        for keyword_in_text in keywords:
            filter_keyword = False
            if len(keyword_in_text) < 3:
                filter_keyword = True
            else:
                for keyword_to_remove in keywords_to_remove:
                    if keyword_to_remove.lower() in keyword_in_text.lower() or keyword_in_text.lower() in single_words_to_filter:
                        filter_keyword = True
                        break
            if not filter_keyword:
                keywords_filtered.append(keyword_in_text)
        return keywords_filtered

    def list_str_to_list(self, list_str):
        list_str = list_str.strip("[]")
        split_indices = []
        index_open = True
        quote_type = ""
        split_index = []
        for i, char in enumerate(list_str):
            if char == "'" or char == '"':
                if index_open:
                    quote_type = char
                    split_index.append(i)
                    index_open = False
                # if we want a closing index two conditions must be fit 1. closing char must be equal to opening char + (comma char in next 3 chars or string ends in the next 3 chars)
                elif ("," in list_str[i:i+4] or i+4 > len(list_str)) and char == quote_type: 
                    split_index.append(i)
                    split_indices.append(split_index)
                    # reset split index
                    split_index = []
                    # search for next index to open list element
                    index_open = True
        list_return = []
        for split_index in split_indices:
            list_element = list_str[split_index[0]:split_index[1]]
            list_return.append(list_element)
        return list_return


    def append_niche_table_in_bigquery(self, marketplace, df, date):
        #df = pd.read_csv("~/shirts.csv",converters={"keywords": lambda x: x.strip("[]").split(", ")})        
        
        # extract product listings as list
        df["product_features"] = df.apply(lambda x: self.list_str_to_list(x["product_features"]), axis=1)

        # keyword to filter (To often used and are not related to niche)
        keywords_to_remove_de = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "Geschenk", "Geschenkidee", "Design", "Weihnachten", "Frau",
        "Geburtstag", "Freunde", "Sohn", "Tochter", "Vater", "Geburtstagsgeschenk", "Herren", "Frauen", "Mutter", "Schwester", "Bruder", "Kinder", 
        "Spruch", "Fans", "Party", "Geburtstagsparty", "Familie", "Opa", "Oma", "Liebhaber", "Freundin", "Freund", "Jungen", "Mädchen", "Outfit",
        "Motiv", "Damen", "Mann", "Papa", "Mama", "Onkel", "Tante", "Nichte", "Neffe", "Jungs", "gift", "Marke", "Kind", "Anlass", "Jubiläum"
        , "Überraschung"]
        keywords_to_remove_en = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "gift", "Brand"]
        keywords_to_remove_dict = {"de": keywords_to_remove_de, "com": keywords_to_remove_en}
        keywords_to_remove = keywords_to_remove_dict[marketplace]

        tr4w_de = TextRank4Keyword(language="de")
        tr4w_en = TextRank4Keyword(language="en")
        keywords_asin = {}
        keywords_count = {}
        keywords_bsr_last = {}
        keywords_price_last = {}
        keywords_bsr_change = {}
        keywords_trend = {}
        time_detect_lang = 0
        for i, df_row in df.iterrows():
            if i % 100 == 0:
                print("Shirt {} of {}".format(i, len(df)))
            try:
                asin = df_row["asin"]
                bsr_last = df_row["bsr_last"]
                bsr_change = df_row["bsr_change"]
                price_last = df_row["price_last"]
                trend_nr = df_row["trend_nr"]
                title = df_row["title"]
                brand = df_row["brand"]
                description = df_row["description"]
                if description == None or type(description) != str or (type(description) == float and np.isnan(description)):
                    description = ""
                language = df_row["language"]
            except Exception as e:
                print(str(e))
                continue

            product_features = [v.strip("'").strip('"') for v in df_row["product_features"]]

            # create text with keyword
            count_feature_bullets = len(product_features)
            # if 5 bullets exists choose only top two (user generated)
            if count_feature_bullets >= 5:
                product_features = product_features[0:2]
            # if 4 bullets exists choose only top one
            elif count_feature_bullets == 4:
                print("asin {} index {} has 4 feature bullets".format(df_row["asin"], i))
                product_features = product_features[0:1]
            # if less than 4 choose no bullet
            else:
                print("asin {} index {} has less than 4 feature bullets".format(df_row["asin"], i))
                product_features = []
            try:
                text = " ".join([title + "."] + [brand + "."] + product_features + [description])
            except Exception as e:
                print(str(e))
                continue

            # language of design
            if language == None or language == "":
                try:
                    time_start = time.time()
                    language = detect(text)
                    time_detect_lang = time_detect_lang + (time.time() - time_start)
                except:
                    continue

            # get all keywords
            if language == "en":
                keywords = tr4w_en.get_unsorted_keywords(text, candidate_pos = ['NOUN', 'PROPN'], lower=False)
            else:
                keywords = tr4w_de.get_unsorted_keywords(text, candidate_pos = ['NOUN', 'PROPN'], lower=False)
            
            # filter keywords
            keywords_filtered = self.filter_keywords(keywords, keywords_to_remove)

            for keyword in keywords_filtered:
                #if uncommented duplicates of asins appear
                #keyword = keyword.replace("'","").replace('"','')
                if keyword in keywords_count:
                    try:
                        keywords_count[keyword] = keywords_count[keyword] + 1
                        keywords_asin[keyword].append(asin)
                        keywords_bsr_last[keyword].append(bsr_last)
                        keywords_trend[keyword].append(trend_nr)
                        keywords_price_last[keyword].append(price_last)
                        keywords_bsr_change[keyword].append(bsr_change)
                    except Exception as e:
                        print(str(e))
                        continue
                else:
                    keywords_count[keyword] = 1
                    keywords_asin[keyword] = [asin]
                    keywords_bsr_last[keyword] = [bsr_last]
                    keywords_trend[keyword] = [trend_nr]
                    keywords_price_last[keyword] = [price_last]
                    keywords_bsr_change[keyword] = [bsr_change]

        df_keywords = self.keyword_dicts_to_df(keywords_asin, keywords_count, keywords_bsr_last, keywords_trend, keywords_price_last, keywords_bsr_change)
        df_keywords["date"] = date
        df_keywords = df_keywords[df_keywords["count_with_bsr"] > 1]
        df_keywords.to_gbq("mba_" + str(marketplace) +".niches", project_id="mba-pipeline", if_exists="append")

    def get_sql_keyword_data(self, marketplace):
        SQL_STATEMENT = '''SELECT t0.asin, t0.brand, t0.title, t0.product_features, t0.description, DATE_DIFF(current_date(), Date(t0.upload_date), DAY) as time_since_upload, t1.language, t0.timestamp FROM `mba-pipeline.mba_{0}.products_details` t0
            LEFT JOIN  `mba-pipeline.mba_{0}.products_language` t1 on t0.asin = t1.asin
            LEFT JOIN  `mba-pipeline.mba_{0}.products_trademark` t2 on t0.brand = t2.brand
            -- get only keywords from not trademarked designs
            WHERE t2.trademark IS NULL
            order by t0.timestamp desc
        '''.format(marketplace)
        return SQL_STATEMENT

    def update_niches(self, marketplace, chunk_size=1000, dates=[]):
        print("Load shirt data from bigquery for niche update")
        start_time_first = time.time()
        project_id = 'mba-pipeline'
        # read data from bigquery
        #df_keyword_data = pd.read_gbq(self.get_sql_keyword_data(marketplace), project_id="mba-pipeline").drop_duplicates(["asin"])
        df_keyword_data = pd.read_csv("~/keyword_data.csv")

        print("Chunk size: "+ str(chunk_size))
        # create dataframe with asins and timestamp as index. Will be used for chunking data to prevent reading all daily bsr data from bigquery
        df_shirts_asin = df_keyword_data[["asin", "timestamp"]].copy().set_index('timestamp')

        # older dates: "2020-06-15"
        if len(dates) == 0:
            dates = [str(datetime.now().date())]

        # dev case
        #df_keywords_data_with_more_info = pd.read_csv("~/shirts_20200615.csv")
        #self.append_niche_table_in_bigquery(marketplace, df_keywords_data_with_more_info, dates[0])

        for date in dates:
            print("Date %s" % date)
            # get string of next day of day
            date_object = datetime.strptime(date, "%Y-%m-%d")
            date_object = date_object + timedelta(days=1)
            next_day = datetime.strftime(date_object, "%Y-%m-%d")

            df_shirts_asin_chunks = [df_shirts_asin.loc[date:][i:i+chunk_size] for i in range(0,df_shirts_asin.loc[date:].shape[0],chunk_size)]
            for i, df_shirts_asin_chunk in enumerate(df_shirts_asin_chunks):
                start_time = time.time()
                print("Chunk %s of %s" %(i, len(df_shirts_asin_chunks)))
                asin_list = df_shirts_asin_chunk["asin"].tolist()
                print("Start to get chunk from bigquery")
                try:
                    self.df_shirts_detail_daily = pd.read_gbq(self.get_sql_shirts_detail_daily(marketplace,asin_list=asin_list, until_date=next_day), project_id="mba-pipeline", verbose=True).drop_duplicates()
                except Exception as e:
                    print(str(e))
                    raise e
                print("Got bigquery chunk")
                self.df_shirts_detail_daily["date"] = self.df_shirts_detail_daily.apply(lambda x: x["timestamp"].date(), axis=1)

                print("Start to get first and last bsr of shirts")
                df_additional_data = df_shirts_asin_chunk.apply(lambda x: pd.Series(self.get_first_and_last_data(x["asin"], with_asin=True)), axis=1)
                df_additional_data.columns=["bsr_last", "price_last", "bsr_first", "price_first", "bsr_change", "bsr_change_total", "price_change", "update_last", "asin"]

                df_keywords_data_chunk = df_keyword_data.merge(df_additional_data, 
                    left_on="asin", right_on="asin")
                if i == 0:
                    df_keywords_data_with_more_info = df_keywords_data_chunk
                else:
                    df_keywords_data_with_more_info = df_keywords_data_with_more_info.append(df_keywords_data_chunk)
                print("elapsed time for chunk: %.2f sec" %((time.time() - start_time)))
            # create trend column
            df_keywords_data_with_more_info = df_keywords_data_with_more_info.merge(self.make_trend_column(df_keywords_data_with_more_info)[["asin","trend_nr"]], on="asin", how='left')
            df_keywords_data_with_more_info = df_keywords_data_with_more_info.reset_index(drop=True)
            print("elapsed time for all chunks: %.2f sec" %((time.time() - start_time_first)))
            # filter keywords to niches and append it to bigquery
            self.append_niche_table_in_bigquery(marketplace, df_keywords_data_with_more_info, date)

    def drop_asins_already_detected(self, df, marketplace):
        return df[~df['asin'].isin(pd.read_gbq("SELECT DISTINCT asin FROM mba_{}.products_language".format(marketplace), project_id="mba-pipeline")["asin"].tolist())]

    def update_language_code(self, marketplace):
        df = pd.read_gbq("SELECT DISTINCT asin, title, product_features FROM mba_{}.products_details".format(marketplace), project_id="mba-pipeline")
        df = self.drop_asins_already_detected(df, marketplace)
        df = df.drop_duplicates(["asin"])
        df["language"] = "de"
        for i, df_row in df.iterrows():
            title = df_row["title"]
            if self.count_slashes(df_row["product_features"]) > 5:
                product_features = [v.strip("''") for v in df_row["product_features"].strip("[]").split(", \'")]
            else:
                product_features = [v.strip("''") for v in df_row["product_features"].strip("[]").split("',")]

            count_feature_bullets = len(product_features)
            if count_feature_bullets >= 5:
                product_features = product_features[0:2]
            elif count_feature_bullets == 4:
                print("asin {} index {} has 4 feature bullets".format(df_row["asin"], i))
                product_features = product_features[0:1]
            else:
                print("asin {} index {} has less than 4 feature bullets".format(df_row["asin"], i))
                product_features = []
            text = " ".join([title + "."] + product_features)
            try:
                language = detect(text)
            except:
                continue
            if language == "en":
                df.loc[i, "language"] = language
        
        df[["asin", "language"]].to_gbq("mba_{}.products_language".format(marketplace), project_id="mba-pipeline", if_exists="append")

    def update_trademark(self, marketplace):
        df = pd.read_gbq("SELECT DISTINCT brand, count(*) as count FROM mba_{}.products_details group by brand order by count desc".format(marketplace), project_id="mba-pipeline")
        df["trademark"] = True
        trademarks = ["disney", "star wars", "marvel", "warner bros", "dc comics", "besuchen sie den", "cartoon network", "fx networks", "jurassic world",
        "wizarding world", "naruto", "peanuts", "looney tunes", "jurassic park", "20th century fox tv", "transformers", "grumpy cat", "nickelodeon",
        "harry potter", "my little pony", "pixar", "stranger things", "netflix", "the walking dead", "wwe", "world of tanks", "motorhead", "iron maiden"
        , "bob marley", "rise against", "roblox", "tom & jerry", "outlander", "care bears", "gypsy queen", "werner", "the simpsons", "Breaking Bad", "Slayer Official",
        "Power Rangers", "Guns N Roses", "Black Sabbath", "Justin Bieber", "Kung Fu Panda", "BTS", "Britney Spears", "Winx", "Dungeons & Dragons", "super.natural"
        "Terraria", "Teletubbies", "Slipknot", "Woodstock", "Shaun das schaf"]
        df_trademarks = df[df["brand"].str.contains("|".join(trademarks),regex=True, case=False)]
        df_trademarks[["brand", "trademark"]].to_gbq("mba_{}.products_trademark".format(marketplace), project_id="mba-pipeline", if_exists="replace")





