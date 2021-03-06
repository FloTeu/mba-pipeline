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
import difflib
from pytz import timezone
import subprocess
import collections
from niche_updater import list_str_to_list
import hashlib
import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

class DataHandler():
    def __init__(self, marketplace="de"):
        self.filePath = None
        self.df_shirts_detail_daily = None
        # keyword to filter (To often used and are not related to niche)
        self.marketplace = marketplace
        self.keywords_to_remove_de = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "Geschenk", "Geschenkidee", "Design", "Weihnachten", "Frau",
        "Geburtstag", "Freunde", "Sohn", "Tochter", "Vater", "Geburtstagsgeschenk", "Herren", "Frauen", "Mutter", "Schwester", "Bruder", "Kinder", 
        "Spruch", "Fans", "Party", "Geburtstagsparty", "Familie", "Opa", "Oma", "Liebhaber", "Freundin", "Freund", "Jungen", "Mädchen", "Outfit",
        "Motiv", "Damen", "Mann", "Papa", "Mama", "Onkel", "Tante", "Nichte", "Neffe", "Jungs", "gift", "Marke", "Kind", "Anlass", "Jubiläum"
        , "Überraschung"]
        self.keywords_to_remove_en = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "gift", "Brand", "family", "children", "friends", "sister", "brother",
         "childreen", "present", "boys", "girls"]
        self.keywords_to_remove_dict = {"de": self.keywords_to_remove_de, "com": self.keywords_to_remove_en}
        self.keywords_to_remove = self.keywords_to_remove_dict[marketplace]
        self.keywords_to_remove_lower = [v.lower() for v in self.keywords_to_remove_dict[marketplace]]

        self.tr4w_de = TextRank4Keyword(language="de")
        self.tr4w_en = TextRank4Keyword(language="en")

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
        order by t_fin.bsr_mean desc
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
        SELECT t0.asin, t0.price, t0.bsr, CAST(REPLACE(t1.price, ',', '.') as FLOAT64) as price_overview, t0.array_bsr_categorie,
         t0.customer_review_score_mean, t0.customer_review_count, t0.timestamp
        FROM `mba-pipeline.mba_{0}.products_details_daily` t0
        LEFT JOIN (SELECT distinct asin, price FROM `mba-pipeline.mba_{0}.products`) t1 on t1.asin = t0.asin
        where t0.asin in {1} {3}
        order by t0.asin, t0.timestamp desc
        {2}
        """.format(marketplace, SQL_WHERE_IN, SQL_LIMIT, until_time)
        return SQL_STATEMENT

    def power(self, my_list):
        '''Exponential growth
        '''
        return [ x**3 for x in my_list ]

    def change_outlier_with_max(self, list_with_outliers, q=90):
        value = np.percentile(list_with_outliers, q)
        print(value)
        for i in range(len(list_with_outliers)):
            if list_with_outliers[i] > value:
                list_with_outliers[i] = value
        #return list_with_outliers

    def add_value_to_older_shirts(self, x_scaled, index_privileged, add_value, add_value_newer=0.05):
        for i in range(len(x_scaled)):
            if i > index_privileged:
                x_scaled[i] = x_scaled[i] + add_value
            else:
                x_scaled[i] = x_scaled[i] + add_value_newer

    def make_trend_column(self, df_shirts, months_privileged=6, marketplace="de"):
        df_shirts = df_shirts.sort_values("time_since_upload").reset_index(drop=True)
        # get list of integers with time since upload days
        x = df_shirts[["time_since_upload"]].values 
        # fill na with max value
        x = np.nan_to_num(x, np.nanmax(x))
        # get index of last value within privileged timezone
        index_privileged = len([v for v in x if v < 30*months_privileged]) - 1
        # transform outliers to max value before outliers
        x_without_outliers = x.copy()
        self.change_outlier_with_max(x_without_outliers)
        min_max_scaler = preprocessing.MinMaxScaler()
        # scale list to values between 0 and 1
        x_scaled = min_max_scaler.fit_transform(x_without_outliers)
        # add add_value to scaled list to have values < 1 reduce trend and values < 1 increase it
        add_value = 1 - x_scaled[index_privileged]
        self.add_value_to_older_shirts(x_scaled, index_privileged, add_value)
        #x_scaled = x_scaled + add_value
        # power operation for exponential change 0 < x < (1+add_value)**3
        x_power = self.power(x_scaled)
        df = pd.DataFrame(x_power)
        df_shirts["time_since_upload_power"] = df.iloc[:,0]
        df_shirts.loc[(df_shirts['bsr_category'] != self.get_category_name(marketplace)), "bsr_last"] = 999999999
        df_shirts.loc[(df_shirts['bsr_last'] == 0.0), "bsr_last"] = 999999999
        df_shirts.loc[(df_shirts['bsr_last'] == 404.0), "bsr_last"] = 999999999
        df_shirts["trend"] = df_shirts["bsr_last"] * df_shirts["time_since_upload_power"]
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

    def get_takedown_lists(self, df_asins):
        takedown_data = df_asins.apply(lambda x: self.get_takedown_data(x), axis=1)
        takedown_list = []
        takedown_date_list = []
        for takedown_data_i in takedown_data:
            takedown_list.append(takedown_data_i[0])
            takedown_date_list.append(takedown_data_i[1])
        return takedown_list, takedown_date_list

    def get_plot_lists(self, df_asins):
        plot_data = df_asins.apply(lambda x: self.create_plot_data(x), axis=1)
        plot_x=[]
        plot_y=[]
        for plot_data_i in plot_data:
            plot_x.append(plot_data_i[0])
            plot_y.append(plot_data_i[1])
        plot_price_data = df_asins.apply(lambda x: self.create_plot_price_data(x), axis=1)
        plot_x_price = []
        plot_y_price = []
        for plot_price_data_i in plot_price_data:
            plot_x_price.append(plot_price_data_i[0])
            plot_y_price.append(plot_price_data_i[1])
        return plot_x, plot_y, plot_x_price, plot_y_price

    def update_bq_shirt_tables(self, marketplace, chunk_size=500, limit=None, filter=None,dev=False):
        # This part should only triggered once a day to update all relevant data
        print("Load shirt data from bigquery")
        start_time = time.time()
        project_id = 'mba-pipeline'
        bq_client = bigquery.Client(project=project_id)
        df_shirts = pd.read_gbq(self.get_sql_shirts(marketplace, None, None), project_id="mba-pipeline").drop_duplicates(["asin"])
        #df_shirts = df_shirts[df_shirts["asin"]== "B08XPGNNVP"]
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
            plot_x, plot_y, plot_x_price, plot_y_price = self.get_plot_lists(df_shirts_asin_chunk)
            print("elapsed time: %.2f sec" %((time.time() - start_time)))
 
            print("Start to get first and last bsr of shirts")
            start_time = time.time()
            df_additional_data = df_shirts_asin_chunk.apply(lambda x: pd.Series(self.get_first_and_last_data(x["asin"], marketplace=marketplace)), axis=1)
            df_additional_data.columns=["bsr_last", "price_last", "bsr_first", "price_first", "bsr_change", "bsr_change_total", "price_change", "update_last", "score_last", "score_count", "bsr_category"]
            df_additional_data["plot_x"],df_additional_data["plot_y"] = plot_x, plot_y
            df_additional_data["plot_x_price"],df_additional_data["plot_y_price"] = plot_x_price, plot_y_price
            # add takedown data    
            takedown_list, takedown_date_list = self.get_takedown_lists(df_shirts_asin_chunk)        
            df_additional_data["takedown"],df_additional_data["takedown_date"] = takedown_list, takedown_date_list

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
        # try to create should_be_updated column
        try:
            df_shirts_with_more_info['bsr_last_old'] = df_shirts_with_more_info['bsr_last_old'].fillna(value=0).astype(int)
            df_shirts_with_more_info['bsr_last_change'] = (df_shirts_with_more_info["bsr_last_old"] - df_shirts_with_more_info["bsr_last"]).astype(int)
            # get the date one week ago
            date_one_week_ago = (datetime.now() - timedelta(days = 7)).date()
            # filter df which should always be updated (update newer than 7 days + bsr_count equals 1 or 2 or trend_nr lower or equal to 2000) 
            df_should_update = df_shirts_with_more_info[((df_shirts_with_more_info["bsr_count"]<=2) & (df_shirts_with_more_info["update_last"]>=date_one_week_ago)) | (df_shirts_with_more_info["trend_nr"]<=2000)]
            # change bsr_last_change to 1 for those how should be updated independent of bsr_last
            df_shirts_with_more_info.loc[df_should_update.index, "bsr_last_change"] = 1
            df_shirts_with_more_info['should_be_updated'] = df_shirts_with_more_info['bsr_last_change'] != 0
        except Exception as e:
            df_shirts_with_more_info["should_be_updated"] = True

        # save dataframe with shirts in local storage
        print("Length of dataframe", len(df_shirts_with_more_info),dev_str)
        df_shirts_with_more_info.to_gbq("mba_" + str(marketplace) +".merchwatch_shirts" + dev_str,chunksize=10000, project_id="mba-pipeline", if_exists="replace")
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

    def get_category_name(self, marketplace):
        if marketplace == "de":
            return "Bekleidung"
        else:
            return "Clothing, Shoes & Jewelry"

    def get_bsr_category(self, df_row, marketplace):
        bsr_category = df_row["array_bsr_categorie"].strip("[]").split(",")[0].strip("'")
        if bsr_category == "404" or bsr_category == "":
            bsr_category = self.get_category_name(marketplace)
        return bsr_category

    def get_first_and_last_data(self, asin, with_asin=False, marketplace="de"):
        # return last_bsr, last_price, first_bsr, first_price
        occurences = (self.df_shirts_detail_daily.asin.values == asin)
        df_occ = self.df_shirts_detail_daily[occurences]
        if len(df_occ) == 0:
            category_name = self.get_category_name(marketplace)
            if with_asin:
                return 0,0,0,0,0,0,0,0,0,0, category_name, asin
            else:
                return 0,0,0,0,0,0,0,0,0,0, category_name
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
        bsr_category = self.get_bsr_category(last_occ, marketplace)

        if with_asin:
            return last_occ["bsr"], price_last, first_occ["bsr"], first_occ_price_ue_zero["price"], self.get_change_total(last_occ["bsr"], occ_4w["bsr"]), self.get_change_total(last_occ["bsr"], first_occ["bsr"]), self.get_change_total(last_occ["price"], first_occ["price"]), last_occ["date"], last_occ["customer_review_score_mean"], last_occ["customer_review_count"], bsr_category, asin
        else:
            return last_occ["bsr"], price_last, first_occ["bsr"], first_occ_price_ue_zero["price"], self.get_change_total(last_occ["bsr"], occ_4w["bsr"]), self.get_change_total(last_occ["bsr"], first_occ["bsr"]), self.get_change_total(last_occ["price"], first_occ["price"]), last_occ["date"], last_occ["customer_review_score_mean"], last_occ["customer_review_count"], bsr_category

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
        df_asin_detail_daily = self.df_shirts_detail_daily[self.df_shirts_detail_daily["asin"]==df_shirts_row["asin"]]
        # remove bsr with 0 or 404
        df_asin_detail_daily = df_asin_detail_daily[(df_asin_detail_daily["bsr"]!=0)&(df_asin_detail_daily["bsr"]!=404)]
        # drop bsr data with same date (multiple times crawled on same day)
        df_asin_detail_daily = df_asin_detail_daily.drop_duplicates(["date"])
        x=",".join(x.strftime("%d/%m/%Y") for x in df_asin_detail_daily["date"].tolist())
        y=",".join(str(y) for y in df_asin_detail_daily["bsr"].tolist())
        return x, y

    def create_plot_price_data(self, df_shirts_row):
        df_asin_detail_daily = self.df_shirts_detail_daily[self.df_shirts_detail_daily["asin"]==df_shirts_row["asin"]]
        price_dates = df_asin_detail_daily["date"].tolist()
        price_dates.reverse()
        price_data = df_asin_detail_daily["price"].tolist()
        price_data.reverse()
        x = ""
        y = ""
        price_last = 0
        for i, price in enumerate(price_data):
            # set only new price data if its new (in relation to last price) and unequal to 404 or lower than 10
            if price != price_last and price < 60 and price > 10:
                x = x + price_dates[i].strftime("%d/%m/%Y") + ","
                y = y + "%.2f" % price + ","
            # update price_last only if price is real
            if price < 60 and price > 10:
                price_last = price
        # reverse back to original order
        x_list = x.strip(",").split(",")
        y_list = y.strip(",").split(",")
        x_list.reverse()
        y_list.reverse()
        x = ",".join(x_list)
        y = ",".join(y_list)
        return x, y

    def get_takedown_data(self, df_shirts_row):
        """
            Return: is_takedown (bool), takedown_date (None if is_takedown is False)
        """
        df_asin_detail_daily = self.df_shirts_detail_daily[self.df_shirts_detail_daily["asin"]==df_shirts_row["asin"]]
        price_dates = df_asin_detail_daily["date"].tolist()
        price_data = df_asin_detail_daily["price"].tolist()
        for i, price in enumerate(price_data):
            if price == 404:
                return True, price_dates[i]
        return False, None

    def get_shirt_dataset_sql(self, marketplace, dev=False, update_all=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        ORDERBY_STATEMENT = "order by trend_nr"
        WHERE_STATEMENT = "where bsr_category='{}'".format(self.get_category_name(marketplace))
        if not update_all:
            WHERE_STATEMENT = WHERE_STATEMENT + " and t_fin.should_be_updated"
        SQL_STATEMENT = """
        SELECT t_fin.* FROM (
            SELECT t_tmp.*, ROW_NUMBER() OVER() row_number FROM (
                SELECT t0.*, t_key.keywords, t_lang.language, t_general.description, t2.url_affiliate,t2.img_affiliate, CASE WHEN t2.img_affiliate IS NOT NULL THEN true ELSE false END as affiliate_exists FROM 
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
            left join `mba-pipeline.mba_{0}.products_details_keywords` t_key on t0.asin = t_key.asin
            left join `mba-pipeline.mba_{0}.products_language` t_lang on t0.asin = t_lang.asin
            left join `mba-pipeline.mba_{0}.products_details` t_general on t0.asin = t_general.asin
                
            WHERE t0.upload_date IS NOT NULL --old where t0.bsr_mean != 0 and t0.price_last != 404
             {1}
            ) t_tmp
        ) t_fin {3}
        
        """.format(marketplace, ORDERBY_STATEMENT, dev_str, WHERE_STATEMENT)

        return SQL_STATEMENT

    def get_shirt_dataset_unequal_normal_bsr_sql(self, marketplace, dev=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        WHERE_STATEMENT = "where bsr_category!='{}'".format(self.get_category_name(marketplace))
        SQL_STATEMENT = """
        SELECT asin FROM `mba-pipeline.mba_{0}.merchwatch_shirts{1}` {2}     
        """.format(marketplace, dev_str, WHERE_STATEMENT)
    	#.drop_duplicates(["asin"])
        return SQL_STATEMENT 

    def get_shirt_dataset(self, marketplace, dev=False, update_all=False):
        shirt_sql = self.get_shirt_dataset_sql(marketplace, dev=dev, update_all=update_all)
        try:
            df_shirts=pd.read_gbq(shirt_sql, project_id="mba-pipeline")
        except Exception as e:
            print(str(e))
            raise e
        return df_shirts

    def cut_product_feature_list(self, product_features_list):
        # count number of bullets
        count_feature_bullets = len(product_features_list)
        # if 5 bullets exists choose only top two (user generated)
        if count_feature_bullets >= 5:
            product_features_list = product_features_list[0:2]
        # if 4 bullets exists choose only top one
        elif count_feature_bullets == 4:
            product_features_list = product_features_list[0:1]
        # if less than 4 choose no bullet
        else:
            product_features_list = []
        return product_features_list

    def get_keyword_text(self, df_row):
        """ Function to receive all relevant keyword in a text
        """
        brand_list = df_row["brand"].lower().split(" ")
        title_list = df_row["title"].lower().split(" ")
        language = df_row["language"]
        # get only first two feature bullets (listings)
        product_features_list = list_str_to_list(df_row["product_features"])
        product_features_list = [v.strip("'").strip('"') for v in product_features_list]
        product_features_list = self.cut_product_feature_list(product_features_list)
        return " ".join([df_row["title"] + "."] + [df_row["brand"] + "."] + product_features_list)

    def get_all_keywords(self, df_row):
        asin = df_row["asin"].lower()
        # old method
        brand_list = df_row["brand"].lower().split(" ")
        title_list = df_row["title"].lower().split(" ")
        language = df_row["language"]
        # get only first two feature bullets (listings)
        product_features_list = list_str_to_list(df_row["product_features"])
        product_features_list = [v.strip("'").strip('"') for v in product_features_list]
        product_features_list = self.cut_product_feature_list(product_features_list)

        keyword_text = " ".join([df_row["title"] + "."] + [df_row["brand"] + "."] + product_features_list)
        if language == None or language == "":
            try:
                language = detect(keyword_text)
            except:
                pass
        # extract type of token like NOUN or VERB etc.
        if language == "de":
            tr4w = self.tr4w_de
            if self.marketplace=="de":
                keywords_to_remove = self.keywords_to_remove_lower
            else:
                keywords_to_remove = [v.lower() for v in self.keywords_to_remove_de]
        else:
            tr4w = self.tr4w_en
            keywords_to_remove = self.keywords_to_remove_en
        doc = tr4w.nlp(keyword_text)
        keyword_to_pos = {}
        for token in doc:
            keyword_to_pos.update({token.text.lower(): token.pos_})

        # extract product features/ listings
        product_features_keywords_list = []
        for product_features in product_features_list:
            words = re.findall(r'\w+', product_features) 
            product_features_keywords_list.extend([v.lower() for v in words])

        keywords = brand_list + title_list + product_features_keywords_list
        keywords = [word for word in keywords if word.lower() not in keywords_to_remove]

        # list of sentences
        keyword_blocks = [df_row["brand"].lower(), df_row["title"].lower()]
        for product_features_list_i in product_features_list:
            product_feature_sentences = product_features_list_i.split(".")
            for product_feature_sentence in product_feature_sentences:
                if product_feature_sentence.strip() != "":
                    keyword_blocks.append(product_feature_sentence.strip())

        # extract longtail keywords
        keyword_longtail = []
        for keyword_sentence in keyword_blocks:
            words = re.findall(r'\w+', keyword_sentence)
            words = [word.lower() for word in words if word.lower() not in keywords_to_remove + ["t"]]
            keywords_2word = []
            for i in range(len(words)):
                keywords_2word.append(" ".join(words[i:i+2]))
            keywords_3word = []
            for i in range(len(words)):
                keywords_3word.append(" ".join(words[i:i+3]))
            keywords_4word = []
            for i in range(len(words)):
                keywords_4word.append(" ".join(words[i:i+4]))
            keyword_longtail.extend(keywords_2word[0:-1] + keywords_3word[0:-2] + keywords_4word[0:-3])

        # drop duplicates
        keywords_final = list(dict.fromkeys(keywords + keyword_longtail))
        # drop not meaningful keywords
        keywords_final = [asin] + [keyword for keyword in keywords_final if tr4w.is_meaningful_keyword(keyword.lower(), keyword_to_pos)]

        return keywords_final
    
    def create_keywords(self, df_row):
        # if keywords are already known use them
        if df_row["keywords"] != None:
            return df_row["keywords"].split(";")
        return self.get_keywords_filtered(df_row)

    def was_takedown(self, df_row):
        if df_row["price_last"] == 404:
            return True
        else:
            return False
    
    def create_stem_keywords(self, df_row):
        """Function to receive firestore property with stem keywords
        """
        keyword_text = self.get_keyword_text(df_row)
        keyword_list = re.findall(r'\w+', keyword_text)
        fs_stem_word_dict = self.get_fs_stem_word_dict(keyword_list)
        fs_stem_word_dict.update({df_row["asin"].lower(): True})
        return fs_stem_word_dict

    def get_fs_stem_word_dict(self, keywords):
        # return the stem words of given list of keywords
        fs_stem_word_dict = {}
        stem_words = self.keywords_to_stem_words(keywords)
        # drop duplicates
        for stem_word in stem_words:
            fs_stem_word_dict.update({stem_word: True})
        return fs_stem_word_dict

    def keywords_to_stem_words(self, keywords):
        stem_words = []
        
        # drop keywords with more than 1 word
        keywords = [keyword for keyword in keywords if len(keyword.split(" ")) == 1]

        # keywords are not stemped related to the language of the text but of the marketplace they are uploaded to
        # Background: user searches for data in marketplace and might write english keyword but want german designs
        # Use Case: User searches for "home office". keyword text is german but includes keyword "home office".
        # Solution: stem dependend on marketplace not language of keyword text
        if self.marketplace == "de":
            stop_words = set(stopwords.words('german'))  
            keywords_filtered = [w for w in keywords if not w in stop_words]  
            snowball_stemmer = SnowballStemmer("german")

            for keyword in keywords_filtered:
                stem_words.append(snowball_stemmer.stem(keyword))
        # other marketplaces which might only be the american in future
        else:
            stop_words = set(stopwords.words('english'))  
            keywords_filtered = [w for w in keywords if not w in stop_words]  
            snowball_stemmer = SnowballStemmer("english")

            for keyword in keywords_filtered:
                stem_words.append(snowball_stemmer.stem(keyword))

        return stem_words

    # def get_bsr_last_ranges(self, df_row):
    #     bsr_last_ranges = {}
    #     bsr_last = df_row["bsr_last"]
    #     # number which represents bsr range in 100000 steps 
    #     bsr_range_point = int(bsr_last / 100000)
    #     for i in range(20):
    #         if i < bsr_range_point:
    #             bsr_last_ranges.update({str(i): False})
    #         else:
    #             bsr_last_ranges.update({str(i): True})
    #     return bsr_last_ranges

    def get_bsr_last_range(self, df_row):
        bsr_last = df_row["bsr_last"]
        # number which represents bsr range in 100000 steps 
        bsr_range_point = int(bsr_last / 100000)
        # case last bsr is between 0 and 5000000
        if bsr_range_point < 50:
            return bsr_range_point
        # case last bsr is higher than 5000000 or does not exists
        else:
            return 99
        return bsr_range_point

    def get_price_last_ranges(self, df_row):
        price_last_ranges = {}
        price_last = df_row["price_last"]
        # number which represents bsr range in 100000 steps 
        price_range_point = int(price_last)
        for i in np.arange(12,26,1):
            if i < price_range_point:
                price_last_ranges.update({str(i): False})
            else:
                price_last_ranges.update({str(i): True})
        return price_last_ranges

    def get_price_last_range(self, df_row):
        price_last = df_row["price_last"]
        # number which represents bsr range in 100000 steps 
        price_range_point = int(price_last)
        return price_range_point

    def get_price_last_ranges_array(self, df_row):
        price_last_ranges_array = []
        price_last = df_row["price_last"]
        # number which represents bsr range in 100000 steps 
        price_range_point = int(price_last)
        # price_range_point is not allowed to be lower than 13
        if price_range_point < 13:
            price_range_point = 13
        # price_range_point is not allowed to be higher than 25
        if price_range_point >= 25:
            price_range_point = 24
        for price_min in np.arange(13,26,1):
            for price_max in np.arange(13,26,1):
                # if price_min is not smaller than price_max, loop should be continued
                if price_min >= price_max:
                    continue
                if price_min <= price_range_point and price_max > price_range_point:
                    pride_range = int(f"{price_min}{price_max}")
                    price_last_ranges_array.append(pride_range)
        return price_last_ranges_array

    def get_score_last_rounded(self, df_row):
        try:
            return int(round(df_row["score_last"], 0)) 
        except:
            return 0

    def get_firestore_data(self, df_row):
        #takedown = self.was_takedown(df_row)
        keywords = self.get_all_keywords(df_row)
        keywords_meaningful = self.create_keywords(df_row)
        keywords_stem = self.create_stem_keywords(df_row)
        price_last_ranges_array = self.get_price_last_ranges_array(df_row)
        price_last_range = self.get_price_last_range(df_row)
        bsr_last_range = self.get_bsr_last_range(df_row)
        score_last_rounded = self.get_score_last_rounded(df_row)
        return keywords, keywords_meaningful, keywords_stem, price_last_ranges_array, price_last_range, bsr_last_range, score_last_rounded

    def update_firestore(self, marketplace, collection, dev=False, update_all=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        firestore = Firestore(collection + dev_str)
        
        df = self.get_shirt_dataset(marketplace, dev=dev, update_all=update_all)
        #df = df.iloc[df[df["asin"]=="B07HJWVF24"].index.values[0]:df.shape[0]]
        #print(df.shape)
        #df_unequal_normal_bsr = pd.read_gbq(self.get_shirt_dataset_unequal_normal_bsr_sql(marketplace, dev=dev), project_id="mba-pipeline").drop_duplicates(["asin"])

        chunk_size = 1000
        df_chunks = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
        for df_chunk in df_chunks:
            firestore_property_columns = ["keywords", "keywords_meaningful", "keywords_stem", "price_last_ranges_array", "price_last_range", "bsr_last_range", "score_last_rounded"]
            time_start = time.time()
            firestore_data_series = df_chunk.apply(lambda x: self.get_firestore_data(x), axis=1)
            print("elapsed time for all keyword creation %.2f min" % ((time.time() - time_start)/60))
            df_fs_data = pd.DataFrame([[keywords, keywords_meaningful, keywords_stem, price_last_ranges_array, price_last_range, bsr_last_range, score_last_rounded] for keywords, keywords_meaningful, keywords_stem, price_last_ranges_array, price_last_range, bsr_last_range, score_last_rounded in firestore_data_series.values], columns=firestore_property_columns)
            # merge data
            df_chunk = df_chunk.reset_index(drop=True)

            df_chunk = pd.concat([df_chunk, df_fs_data.reindex(df_chunk.index)], axis=1)

            # df_chunk["takedown"] = df_chunk.apply(lambda x: self.was_takedown(x), axis=1)
            # time_start = time.time()
            # df_chunk["keywords_meaningful"] = df_chunk.apply(lambda x: self.create_keywords(x), axis=1)
            # print("elapsed time for all meaningful keywords creation %.2f min" % ((time.time() - time_start)/60))
            # time_start = time.time()
            # df_chunk["keywords"] = df_chunk.apply(lambda x: self.get_all_keywords(x), axis=1)
            # df_chunk["keywords_stem"] = df_chunk.apply(lambda x: self.create_stem_keywords(x), axis=1)
            # df_chunk["price_last_ranges"] = df_chunk.apply(lambda x: self.get_price_last_ranges(x), axis=1)
            # df_chunk["bsr_last_ranges"] = df_chunk.apply(lambda x: self.get_bsr_last_ranges(x), axis=1)

        
            #df_chunk["keywords_meaningful_count"] = df_chunk.apply(lambda x: len(x["keywords_meaningful"]), axis=1)
            columns = list(df_chunk.columns.values)
            for column_to_drop in ["should_be_updated", "product_features", "trend_nr_old", "bsr_last_old", "description", "row_number", "score_min", "score_mean", "score_max"]:
                columns.remove(column_to_drop)
            df_filtered = df_chunk[columns]
            #df_chunk["keywords"] = df_chunk.apply(lambda x: self.get_keywords_filtered(x), axis=1)
            #df_filtered = df_filtered.iloc[124*250:len(df_filtered)]
            firestore.update_by_df_batch(df_filtered, "asin", batch_size=250)
            # for i, df_row in df_unequal_normal_bsr.iterrows():
            #     asin = df_row["asin"]
            #     firestore.delete_document(asin)


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
        bsr_best_list = []
        bsr_change_mean_list = []
        bsr_change_variance_list = []
        bsr_variance_list = []
        price_mean_list = []
        price_variance_list = []
        price_lowest_list = []
        price_heighest_list = []
        trend_mean_list = []
        trend_best_list = []
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
                bsr_best = int(min(bsr_list_filtered))
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
            bsr_best_list.append(bsr_best)
            
            # fill price lists
            price_mean, price_variance = self.get_mean_and_variance(price_list_filtered, return_integer=False)
            price_lowest = min(price_list_filtered)
            price_highest = max(price_list_filtered)
            price_mean_list.append(price_mean)
            price_variance_list.append(price_variance)
            price_lowest_list.append(price_lowest)
            price_heighest_list.append(price_highest)

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
            trend_best = int(min(trend_list_filtered))
            trend_mean_list.append(trend_mean)
            trend_best_list.append(trend_best)
            trend_variance_list.append(trend_variance)
        
        # return final niche dataframe
        return pd.DataFrame({"keyword":keyword_list, "count": count_list, "count_with_bsr": count_with_bsr_list, "count_without_bsr": count_without_bsr_list, "count_404": count_with_404_list,
         "bsr_mean": bsr_mean_list,"bsr_best": bsr_best_list, "bsr_variance": bsr_variance_list, "trend_mean": trend_mean_list, "trend_best":trend_best_list,"trend_variance": trend_variance_list, "bsr_change_mean": bsr_change_mean_list,
         "bsr_change_variance":bsr_change_variance_list, "price_mean": price_mean_list, "price_lowest": price_lowest_list, "price_heighest": price_heighest_list,"price_variance": price_variance_list, "asin": asin_list}
         )#,dtype={'keyword': str,'Wind':int64})
    
    def filter_keywords(self, keywords, single_words_to_filter=["t","du"]):
        keywords_filtered = []
        for keyword_in_text in keywords:
            if keyword_in_text[len(keyword_in_text)-2:len(keyword_in_text)] in [" t", " T"]:
                keyword_in_text = keyword_in_text[0:len(keyword_in_text)-2]
            filter_keyword = False
            if len(keyword_in_text) < 3:
                filter_keyword = True
            else:
                for keyword_to_remove in self.keywords_to_remove:
                    if keyword_to_remove.lower() in keyword_in_text.lower() or keyword_in_text.lower() in single_words_to_filter:
                        filter_keyword = True
                        break
            if not filter_keyword:
                keywords_filtered.append(keyword_in_text)
        return keywords_filtered

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    

    def get_similar_keywords_to_drop(self, keywords_count, chunk_size=2000):
        '''This Function returns a list of keywords which are similar to keywords with higher count/appereance and therefore should be dropped
        '''
        similar_keywords_to_drop = []
        keywords = list(keywords_count.keys())
        keywords_sorted = sorted(keywords, key=str.lower)

        keyword_chunks = self.chunks(keywords_sorted, chunk_size)
        count_chunks = 0
        start_time_total = time.time()
        for keyword_chunk in keyword_chunks:
            count_chunks += 1
            print("Chunk {} with first keyword: {}".format(count_chunks, keyword_chunk[0]))
            start_time = time.time()
            for keyword in keyword_chunk:
                # get all similar keywords
                similar_keywords = difflib.get_close_matches(keyword, keyword_chunk, n=10, cutoff=0.9)
                if len(similar_keywords) > 0:
                    keyword_with_highest_count = similar_keywords[0]
                    highest_count = keywords_count[keyword_with_highest_count]
                    for similar_keyword in similar_keywords:
                        #score = difflib.SequenceMatcher(None, keyword, similar_keyword).ratio()
                        # get count of similar keyword
                        keyword_count = keywords_count[similar_keyword]
                        if keyword_count > highest_count:
                            # found new keyword with highest count
                            keyword_with_highest_count = similar_keyword
                            # set highest count to new highest count found
                            highest_count = keyword_count
                    # remove keyword with highest count from drop list
                    similar_keywords.remove(keyword_with_highest_count)
                similar_keywords_to_drop.extend(similar_keywords)
                # remove keywords from chunk to prevent looping not again through keyword
                for similar_keyword in similar_keywords:
                    keyword_chunk.remove(similar_keyword)
            #print("elapsed time for chunk: %.2f sec" %((time.time() - start_time)))
        print("elapsed time finding similar keywords to drop total: %.2f sec" %((time.time() - start_time_total)))

        # return keywords to drop
        return similar_keywords_to_drop



    def get_keywords_filtered(self, df_row):
        time_detect_lang = 0
        try:
            keywords = df_row["keywords"]
            asin = df_row["asin"]
            title = df_row["title"]
            brand = df_row["brand"]
            description = df_row["description"]
            if description == None or type(description) != str or (type(description) == float and np.isnan(description)):
                description = ""
            language = df_row["language"]
        except Exception as e:
            print(str(e))
            raise e
        
        # if no keyword was extracted before find ones
        do_keywords_not_exist = True
        if type(keywords) == str and len(keywords.split(";"))>0:
            do_keywords_not_exist = False
        else:
            try:
                if keywords == None:
                    do_keywords_not_exist = True
                else:
                    do_keywords_not_exist = np.isnan(keywords) 
            except Exception as e:
                print(str(e), asin)
                pass
        
        if do_keywords_not_exist:
            product_features = [v.strip("'").strip('"') for v in df_row["product_features"]]
            
            # create text with keyword
            product_features = self.cut_product_feature_list(product_features)

            try:
                text = " ".join([title + "."] + [brand + "."] + product_features + [description])
            except Exception as e:
                print(str(e))
                return None

            # language of design
            if language == None or language == "":
                try:
                    time_start = time.time()
                    language = detect(text)
                    time_detect_lang = time_detect_lang + (time.time() - time_start)
                except:
                    return None

            # get all keywords
            if language == "en":
                keywords = self.tr4w_en.get_unsorted_keywords(text, candidate_pos = ['NOUN', 'PROPN'], lower=False)
            else:
                keywords = self.tr4w_de.get_unsorted_keywords(text, candidate_pos = ['NOUN', 'PROPN'], lower=False)
            
            # filter keywords
            keywords_filtered = self.filter_keywords(keywords)

        else:
            keywords_filtered = keywords.split(";")
        
        # drop duplicates (Happens if ; is within keyword and therfore gets splitted twice)
        keywords_filtered = list(dict.fromkeys(keywords_filtered))
        
        return keywords_filtered


    def drop_asins_already_detected(self, df, marketplace):
        return df[~df['asin'].isin(pd.read_gbq("SELECT DISTINCT asin FROM mba_{}.products_language".format(marketplace), project_id="mba-pipeline")["asin"].tolist())]

    def update_language_code(self, marketplace):
        df = pd.read_gbq("SELECT DISTINCT asin, title, product_features FROM mba_{}.products_details".format(marketplace), project_id="mba-pipeline")
        df = self.drop_asins_already_detected(df, marketplace)
        df = df.drop_duplicates(["asin"])
        df["language"] = "de"
        df["product_features"] = df.apply(lambda x: list_str_to_list(x["product_features"]), axis=1)

        for i, df_row in df.iterrows():
            title = df_row["title"]
            product_features = [v.strip("'").strip('"') for v in df_row["product_features"]]
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
        df = pd.read_gbq("SELECT DISTINCT brand FROM mba_{}.products_details group by brand".format(marketplace), project_id="mba-pipeline")
        df["trademark"] = True
        df_listings = pd.read_gbq("SELECT product_features, brand FROM mba_{}.products_details".format(marketplace), project_id="mba-pipeline")
        trademarks = ["disney", "star wars", "marvel", "warner bros", "dc comics", "besuchen sie den", "cartoon network", "fx networks", "jurassic world",
        "wizarding world", "naruto", "peanuts", "looney tunes", "jurassic park", "20th century fox tv", "transformers", "grumpy cat", "nickelodeon",
        "harry potter", "my little pony", "pixar", "stranger things", "netflix", "the walking dead", "wwe", "world of tanks", "motorhead", "iron maiden"
        , "bob marley", "rise against", "roblox", "tom & jerry", "outlander", "care bears", "gypsy queen", "werner", "the simpsons", "Breaking Bad", "Slayer Official",
        "Power Rangers", "Guns N Roses", "Black Sabbath", "Justin Bieber", "Kung Fu Panda", "BTS", "Britney Spears", "Winx", "Dungeons & Dragons", "super.natural"
        "Terraria", "Teletubbies", "Slipknot", "Woodstock", "Shaun das schaf", "Adult Swim", "Despicable Me", "Shrek", "The Thread Shop", "Licensed"]
        #df_trademarks = df[df["brand"].str.contains("|".join(trademarks),regex=True, case=False)]
        df_trademarks = pd.DataFrame(columns=['brand', 'trademark'])
        for trademark in trademarks:
            df_trademarks_row = df[df["brand"].str.contains(trademark, regex=True, case=False)]
            df_listings_trademarked_brands_row = df_listings[df_listings["product_features"].str.contains(" " + trademark + " ", case=False)][["brand"]]
            for i, df_listings_trademarked_brands_row_i in df_listings_trademarked_brands_row.iterrows():
                if df_listings_trademarked_brands_row_i["brand"] not in df_trademarks_row["brand"].tolist():
                    # add brands from listing matches
                    df_trademarks_row = df_trademarks_row.append(df_listings_trademarked_brands_row_i, ignore_index=True)
            df_trademarks_row = df_trademarks_row.reset_index(drop=True)
            df_trademarks_row["trademark"] = trademark
            df_trademarks_row = df_trademarks_row.drop_duplicates(["brand"])
            if df_trademarks.empty:
                df_trademarks = df_trademarks_row.copy()
            else:
                df_trademarks = df_trademarks.append(df_trademarks_row, ignore_index=True)
        
        #df_listings = df_listings[df_listings["product_features"].str.contains("|".join(trademarks),regex=True, case=False)][["brand", "trademark"]].drop_duplicates(["brand"])
        #df_trademarks = df_trademarks.append(df_listings).drop_duplicates(["brand"])
        df_trademarks[["brand", "trademark"]].to_gbq("mba_{}.products_trademark".format(marketplace), project_id="mba-pipeline", if_exists="replace")
        
        # FIRESTORE
        firestore_trademark = Firestore("de_trademarks")
        for trademark in trademarks:
            df_firestore = df_trademarks[df_trademarks["trademark"]==trademark]
            brands = df_firestore["brand"].tolist()
            firestore_dict = {"brands": brands, "trademark": trademark}
            doc_id = hashlib.sha1(trademark.encode("utf-8")).hexdigest()
            doc_ref = firestore_trademark.db.collection(firestore_trademark.collection_name).document(doc_id)
            doc_ref.set(firestore_dict)


    def append_niche_table_in_bigquery(self, marketplace, df, date):
        #df = pd.read_csv("~/shirts.csv",converters={"keywords": lambda x: x.strip("[]").split(", ")})        
        
        # extract product listings as list
        df["product_features"] = df.apply(lambda x: list_str_to_list(x["product_features"]), axis=1)

        df_asin_keywords = pd.read_gbq("SELECT * FROM mba_" + str(marketplace) +".products_details_keywords", project_id="mba-pipeline")
        asins_which_have_already_keywords = df_asin_keywords["asin"].tolist()
        df = df.merge(df_asin_keywords, on='asin', how='left')

        keywords_asin = {}
        keywords_count = {}
        keywords_bsr_last = {}
        keywords_price_last = {}
        keywords_bsr_change = {}
        keywords_trend = {}

        for i, df_row in df.iterrows():
            asin = df_row["asin"]
            bsr_last = df_row["bsr_last"]
            bsr_change = df_row["bsr_change"]
            price_last = df_row["price_last"]
            trend_nr = df_row["trend_nr"]

            keywords_filtered = self.get_keywords_filtered(df_row)
            
            # add Data to dataframe 
            df.loc[i,"keywords"] = ";".join(keywords_filtered)

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
        
        # upload asin + keyword in big query table
        df_asin_keywords = df[["asin","keywords"]]
        # drop asins where keywords are already known
        df_asin_keywords = df_asin_keywords[~df_asin_keywords["asin"].isin(asins_which_have_already_keywords)]
        try:
            df_asin_keywords.to_gbq("mba_" + str(marketplace) +".products_details_keywords", project_id="mba-pipeline", if_exists="append")
        except Exception as e:
            print(str(e))
            pass

        # remove similar keywords
        keywords_count_filtered = {k: v for k, v in keywords_count.items() if v > 1 }
        keywords_to_drop = self.get_similar_keywords_to_drop(keywords_count_filtered)
        print("Found {} keywords to drop".format(len(keywords_to_drop)))
        for keyword in keywords_to_drop:
            keywords_asin.pop(keyword, None)
            keywords_count.pop(keyword, None)
            keywords_bsr_last.pop(keyword, None)
            keywords_trend.pop(keyword, None)
            keywords_bsr_change.pop(keyword, None)

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
        df_keyword_data = pd.read_gbq(self.get_sql_keyword_data(marketplace), project_id="mba-pipeline").drop_duplicates(["asin"])
        #df_keyword_data = pd.read_csv("~/keyword_data.csv")

        print("Chunk size: "+ str(chunk_size))
        # create dataframe with asins and timestamp as index. Will be used for chunking data to prevent reading all daily bsr data from bigquery
        df_shirts_asin = df_keyword_data[["asin", "timestamp"]].copy().set_index('timestamp')

        # older dates: "2020-06-15"
        if len(dates) == 0:
            dates = [str(datetime.now().date())]

        # dev case
        #df_keywords_data_with_more_info = pd.read_csv("~/shirts_20200615.csv")
        #self.append_niche_table_in_bigquery(marketplace, df_keywords_data_with_more_info, dates[0])
        #asin_list = 'B07WSXL51T,B083R3X6N2,B08FC3C2G9,B082G3QJGY,B08MZ4RH8H,B08MZ6DD3R,B08MZ5CYL5,B08MZ49KQR,B08N23KNKQ,B089NXVPN9,B07VJJHGHG,B07KBK8QM9,B081X4CB8L,B08MXVZ1DD,B08JH5ZDQL,B08N7GXTS8,B08N7G36GS,B07WRSDP2S,B08MZ66XLQ,B08N7J8JGT,B08N7JSYM6,B08GK9SV74,B08MWC7J8P,B08MYBDRC3,B08MYQ4MJF,B082BVLXJM,B08KHYYVHR,B07YD3HT71,B089R2T5ZC,B07WY16M1G,B08L1XSGKP,B07K4FBXRQ,B07WDJLVQV,B07XTYZZS5,B07YBJS486,B08125CT2B,B084VQ3Z9S,B08GRZ1689,B082XXD2BN,B0841M5KZX,B0842J49CN,B0842KC6MQ,B0841LS763,B0842MKLQ5,B084SCDY21,B085H2LWPM,B08KSYLJ7D,B08MWF3T1G,B08MVPGCF4,B08BX8GMB6,B08CTJW716,B08FFH11T5,B08FTZSH46,B08JHBMH95,B08JH6TZVN,B08MVC47XK,B08LCW4WT9,B08MXG9F8P,B08MWDMTLM,B08H6FMDL7,B08MMBJMLS,B08MRRGX4J,B08MMJX9RN,B08MNP7T53,B08MMC4KMQ,B08MRFW5M8,B08MMGGZD3,B08MQYH262,B08MS16MZ8,B08MSM8HQB,B08MRXQD93,B08MTW8V9K,B08MX65SLS,B08MVWPL8M,B08MVMDX77,B08MVV33HW,B07JHSZCBH,B08179G4QB,B07Y5C3LGX,B07S1Q9BS4,B084KB95T4,B07ZBSDVX4,B08MRGMCNC,B08F5C82Q3,B08DY7VNGY,B07V43JJY7,B07TTKMLBD,B07S9HG1B9,B08J8XZ44B,B081ZNWRY3,B08FG5XR73,B089TVJLFK,B07PHVFJG4,B083YNP5WN,B08GDFRBFQ,B07TSG1N1J,B0841J2984,B082B7Q1SY,B081FKS914,B081FK6ZVP,B081XLM33D,B089S322YZ,B08CH3H6L7,B089WYN4HH,B089Z3S9PK,B089Z14XGY,B08H9LJQQ8,B08GG57ZFG,B08GZZ56J1,B08GB1LF6X,B07WNWLH83,B089SRSPCG,B089QTWL1Z,B0818MWK44,B0825C9PK4,B08MBNZMQP,B08MBP35DP,B084HG7XCT,B089PS9P36,B084ZQP54S,B089THLJT4,B07SMB73MZ,B07RWF4KWN,B07TVL11M6,B0853VP3GC,B081VTZLLM,B08J5333HZ,B08J14S1T3,B083HY7QGG,B089TZTV19,B0824H73HF,B08CP7RWN4,B07NW6B9P2,B07YXQVLY3,B07MRGTNNF,B0856K4V8N,B08JHGFMMJ,B07WDS48K5,B07WW1RRYQ,B08M49N4GP,B083HXS5XC,B07X1T75X8,B07W6JHXV8,B07XB5X24Q,B07XB31D2S,B07WVDKG2R,B07WXMM3YS,B07Z5L9S3D,B07YD3CWKM,B07YTMJWQM,B081YGBCNM,B07ZHW7H3J,B081FLF5XG,B081FKR9NB,B081FLTS6V,B082GTSPTB,B0842MZ2WQ,B0842KHTH8,B0842SSTZT,B085CSBYXP,B08BBGZRF2,B0866CYCSP,B08DGPDH2H,B08CD3VBBH,B089Z2MMDR,B089WML6TW,B08HJZ3Z1L,B08CSMKB4H,B08CL8H8YC,B08CSNPP9P,B08D5824XD,B08F335KV1,B08F2TW251,B08JQPQQ1B,B08LL1V78Y,B07YBJDHKX,B07NVTQZFB,B07YV59R6Q,B07ZNCTXWQ,B084F8T3VF,B089TKRMV1,B07TL6H9GV,B07NW6LQLF,B07S8R3JV3,B08CP13MH5,B081QY9TX9,B0847DN7K6,B089SLLN7X,B07X4S83KS,B07XXJ292Q,B07WSXL1HG,B07WRSD675,B0841LZ8GF,B07JG1BPRM,B083YNP5WR,B07SDHMKVB,B0814BJ2R6,B07PQ8WV6T,B07WYNNTCT,B07K7T7BKQ,B083SCPWX2,B07L1GBN8J,B08HWM3HHY,B089RDWVCP,B0829SBFZ7,B07YD3QZG5,B08DLQYRC6,B081V1WG1W,B07VY4PPFV,B08B2G7CKW,B089VGG142,B07PDN2V4J,B08DZ1TCQY,B08D8DTWQS,B07X4SDQ5J,B07KCYHP3R,B08J4Y952T,B07FYHLRKG,B08CSVC65S,B081DP2V2F,B07ZS3KBVY,B07ZZR6C5H,B07SP3TRX5,B07SFRV9DF,B07WNXS13Y,B07Q8XRWBQ,B08B2H4HT9,B08D22SFPJ,B08HXKWT3F,B089SLGZ1X,B07Z5BBY9F,B07TGWWFM8,B089THSFZR,B0841XK1Q1,B08CP58R64,B0823JP2G8,B07YD5BTMT,B07PD1BQQK,B07WMY66JD,B07GWTNFHX,B07M936YZD,B08HXQFPC2,B07W61YM6G,B08FYDXRJL,B07XXGTK4C,B08L6PTSG9,B08JVG8F7D,B08K9YZ847,B08JZ5FQHB,B08JCYYDGT,B08JJ2XCKW,B08J8SZ1BF,B08J6F78MS,B08HLHLKK6,B08J8CLXCH,B08J8XLXDX,B08JCWQJL7,B08HWMZ8CZ,B08J15662V,B08H12MW59,B08HK2K5W1,B08GNZ3FK8,B08H1WVGMM,B08F4WZ82L,B08FFPSZB2,B08F5SB2HD,B08GG8QQ4V,B08FYFRBHR,B08FFF5SGT,B08FFPSZCX,B08FFG497T,B08FG478T8,B08FFBC9VB,B08FL83TSL,B08FW6G6ND,B08D8XCTPG,B08DK48RN9,B08DPRL6L3,B08DPBLWZV,B08FC2Y1VV,B08DQP2Y6Y,B08DPGPTZQ,B08CSQ7BNH,B08CXHNGGW,B08CT4FHCZ,B08CSKDWYF,B08D9NC49K,B08D86955Z,B08C8KTMJV,B08C9GPDFK,B08C9P3ZCX,B08C1F717V,B08C761QNH,B08C2KVQ6Q,B08C8KDP6V,B08CJ5GXQH,B08CJ7P1JZ,B08CFZG1VT,B08D1CNB3P,B08BGHTWWV,B08BX1V8CN,B08C2QQ2JR,B08B6G19TV,B08B6LKC25,B08B8RLG88,B08BJY44QP,B08BJGX289,B08BJZ6SNV,B08BLBSRL1,B08BLC14Q2,B08BKPMWK8,B08BKRM31T,B08BQRYKFN,B08BV2CRH9,B08BY2PMLX,B08BY5K9CF,B08B5WX6VN,B08B5Z2V73,B08B9BWVXH,B08B5WPYNW,B08B77TCJH,B08B5XGX1T,B089LGYBFG,B089MJG33B,B089LLN1FL,B089PS9QV4,B08B28MFCW,B089S33GLD,B08B4M6T3D,B08B4QKTQ4,B08656DFKC,B089M13HDN,B0867W91XR,B0867W1W6M,B0868H9B2X,B0869DQX6K,B086BQKYXH,B086CRYH85'.split(",")


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
                asin_list_crawling = self.df_shirts_detail_daily.drop_duplicates(["asin"])["asin"].tolist()

                # drop asins which were not crawled already
                df_shirts_asin_chunk = df_shirts_asin_chunk[df_shirts_asin_chunk["asin"].isin(asin_list_crawling)]

                print("Start to get first and last bsr of shirts")
                df_additional_data = df_shirts_asin_chunk.apply(lambda x: pd.Series(self.get_first_and_last_data(x["asin"], with_asin=True)), axis=1)
                df_additional_data.columns=["bsr_last", "price_last", "bsr_first", "price_first", "bsr_change", "bsr_change_total", "price_change", "update_last", "score_last", "score_count", "bsr_category", "asin"]

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

    def update_niches_by_keyword(self, marketplace, keywords):
        print("Load shirt data from bigquery for niche update")
        start_time_first = time.time()
        project_id = 'mba-pipeline'

        keywords_str = "({})".format(",".join(["'" + v + "'" for v in keywords.split(";")]))
        keywords_list = keywords.split(";")


        SQL_STATEMENT = """SELECT keyword, t1.asin, t1.upload_date, DATE_DIFF(current_date(), Date(t1.upload_date), DAY) as time_since_upload, t1.timestamp FROM (SELECT * FROM `mba-pipeline.mba_{0}.niches`  where keyword in {1}) t0
        CROSS JOIN  `mba-pipeline.mba_{0}.products_details` t1 WHERE t0.asin LIKE CONCAT('%', t1.asin, '%')
            order by t1.timestamp desc""".format(marketplace, keywords_str)
        # read data from bigquery
        df_keyword_data = pd.read_gbq(SQL_STATEMENT, project_id=project_id)#.drop_duplicates(["asin"])

        SQL_STATEMENT = """SELECT date FROM `mba-pipeline.mba_{0}.niches` where keyword in {1}
            order by date desc""".format(marketplace, keywords_str)
        # read data from bigquery
        dates_already_calculated = pd.read_gbq(SQL_STATEMENT, project_id=project_id).drop_duplicates(["date"])["date"].tolist()

        dates_dict = {}
        df_shirts_asin_keyword_dict = {}
        df_shirts_detail_daily_total_dict = {}
        df_shirts_asin = df_keyword_data[["asin", "timestamp"]].copy().set_index('timestamp')
        asin_list = list(set(df_shirts_asin["asin"].tolist()))
        df_shirts_detail_daily_total = pd.read_gbq(self.get_sql_shirts_detail_daily(marketplace,asin_list=asin_list), project_id="mba-pipeline", verbose=True).drop_duplicates()
        
        for keyword in keywords_list:
            df_shirts_asin_keyword = df_keyword_data[df_keyword_data["keyword"]==keyword].drop_duplicates(["asin"])[["asin", "timestamp"]].copy().set_index('timestamp')
            asin_list_keyword = df_shirts_asin_keyword["asin"].tolist()
            df_shirts_detail_daily_total_keyword = df_shirts_detail_daily_total[df_shirts_detail_daily_total["asin"].isin(asin_list_keyword)]
            dates = [str(v.date()) for v in df_shirts_detail_daily_total_keyword.sort_values(["timestamp"])["timestamp"].tolist()]
            # drop duplicates and dates already caclulated
            dates = list(dict.fromkeys(dates))
            for date_already_calculated in dates_already_calculated:
                if date_already_calculated in dates:
                    dates.remove(date_already_calculated)
            dates_dict.update({keyword: dates})
            df_shirts_asin_keyword_dict.update({keyword: df_shirts_asin_keyword})
            df_shirts_detail_daily_total_dict.update({keyword: df_shirts_detail_daily_total_keyword})

        #dates = dates + ["2020-11-30"]
        for keyword in keywords_list:
            dates = dates_dict[keyword]
            #dates = ["2020-11-27"]
            df_shirts_asin = df_shirts_asin_keyword_dict[keyword]
            df_shirts_detail_daily_total = df_shirts_detail_daily_total_dict[keyword]
            for date in dates:
                print("Date %s" % date)
                # get string of next day of day
                date_object = datetime.strptime(date, "%Y-%m-%d")
                date_object = date_object + timedelta(days=1)
                next_day = datetime.strftime(date_object, "%Y-%m-%d")

                df_shirts_asin_date = df_shirts_asin.loc[date:]

                start_time = time.time()
                self.df_shirts_detail_daily = df_shirts_detail_daily_total[df_shirts_detail_daily_total["timestamp"] <= next_day]
                self.df_shirts_detail_daily["date"] = self.df_shirts_detail_daily.apply(lambda x: x["timestamp"].date(), axis=1)
                asin_list_crawling = self.df_shirts_detail_daily.drop_duplicates(["asin"])["asin"].tolist()

                # drop asins which were not crawled already
                df_shirts_asin_date = df_shirts_asin_date[df_shirts_asin_date["asin"].isin(asin_list_crawling)]

                print("Start to get first and last bsr of shirts")
                df_additional_data = df_shirts_asin_date.apply(lambda x: pd.Series(self.get_first_and_last_data(x["asin"], with_asin=True)), axis=1)
                df_additional_data.columns=["bsr_last", "price_last", "bsr_first", "price_first", "bsr_change", "bsr_change_total", "price_change", "update_last", "score_last", "score_count", "bsr_category", "asin"]

                df_keywords_data_with_more_info = df_keyword_data[df_keyword_data["keyword"]==keyword].drop_duplicates(["asin"]).merge(df_additional_data, 
                    left_on="asin", right_on="asin")
                print("elapsed time: %.2f sec" %((time.time() - start_time)))
                
                # create trend column
                df_keywords_data_with_more_info = df_keywords_data_with_more_info.merge(self.make_trend_column(df_keywords_data_with_more_info)[["asin","trend_nr"]], on="asin", how='left').drop_duplicates(["asin"])
                df_keywords_data_with_more_info = df_keywords_data_with_more_info.reset_index(drop=True)
                df_keywords_data_with_more_info["trend_nr"] = -99
                # filter keywords to niches and append it to bigquery
                #self.append_niche_table_in_bigquery(marketplace, df_keywords_data_with_more_info, date)

                keywords_asin = {}
                keywords_count = {}
                keywords_bsr_last = {}
                keywords_price_last = {}
                keywords_bsr_change = {}
                keywords_trend = {}

                for i, df_row in df_keywords_data_with_more_info.iterrows():
                    asin = df_row["asin"]
                    bsr_last = df_row["bsr_last"]
                    bsr_change = df_row["bsr_change"]
                    price_last = df_row["price_last"]
                    trend_nr = df_row["trend_nr"]


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
                try:
                    if df_keywords["price_mean"].iloc[0] < 13:
                        test = 0
                        pass
                    if not df_keywords.empty:
                        df_keywords.to_gbq("mba_" + str(marketplace) +".niches", project_id="mba-pipeline", if_exists="append")
                except Exception as e:
                    print(str(e))
                    pass


