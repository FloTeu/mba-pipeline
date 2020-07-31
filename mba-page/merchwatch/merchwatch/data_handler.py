import pandas as pd
from google.cloud import bigquery
import itertools
from sklearn import preprocessing
import os 
from datetime import date
import re
import datetime 
import time
from plotly.offline import plot
import plotly.graph_objs as go
from plotly.graph_objs import Scatter 
from plotly.graph_objs import Layout 
import gc


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
    SELECT t0.*, t2.title, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t1.url_mba_hq, t1.url_mba_lowq FROM (
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
        
        SELECT t0.*, t2.title, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t1.url_mba_hq, t1.url_mba_lowq FROM (
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

    def get_sql_plots(self, marketplace, asin_list):
            SQL_WHERE_IN = "('" + "','".join(asin_list) + "')"
            SQL_STATEMENT = """
            SELECT asin, plot FROM `mba-pipeline.mba_{0}.plots`
            where asin in {1}
            """.format(marketplace, SQL_WHERE_IN)
            return SQL_STATEMENT

    def get_df_plots(self, marketplace, asin_list):
            project_id = 'mba-pipeline'
            bq_client = bigquery.Client(project=project_id)
            df_shirts_plots = bq_client.query(self.get_sql_plots("de", asin_list)).to_dataframe()
            return df_shirts_plots 

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

    def filter_shirts_by_correct_data(self, df_shirts):
        return df_shirts.loc[(df_shirts['bsr_last'] != 0.0) & (df_shirts['bsr_last'] != 404.0) & (df_shirts['bsr_last'] != 999999999)  & (df_shirts['price_last'] != 404.0) & (df_shirts['price_last'] != 0.0)]

    def get_min_max_dict(self, df_shirts):
        dict_min_max = {}
        df_shirts = self.filter_shirts_by_correct_data(df_shirts)
        columns = df_shirts.columns.values
        for column in columns:
            dict_min_max[column] = [df_shirts[column].min(),df_shirts[column].max()]
        return dict_min_max

    def check_if_shirts_today_exist(self, file_path):
        try:
            date_creation = time.ctime(os.path.getctime(file_path))
            return date.today() == datetime.datetime.strptime(date_creation, "%a %b %d %H:%M:%S %Y").date()
        except:
            return False

    def get_shirts(self, marketplace, limit=None, in_test_mode=False, filter=None):
        print(os.getcwd())
        file_path = "merchwatch/data/shirts.csv"
        # If data already loaded today return it
        if self.check_if_shirts_today_exist(file_path):
            print("Data already loaded today")
            df_shirts=pd.read_csv("merchwatch/data/shirts.csv", sep="\t")
            
            #df_shirts_detail_daily=pd.read_csv("merchwatch/data/shirts_detail_daily.csv", sep="\t")
        else:
            # This part should only triggered once a day to update all relevant data
            print("Load shirt data from bigquery")
            start_time = time.time()
            project_id = 'mba-pipeline'
            bq_client = bigquery.Client(project=project_id)
            df_shirts = bq_client.query(self.get_sql_shirts(marketplace, limit, filter)).to_dataframe().drop_duplicates()
            df_shirts_with_more_info = df_shirts.copy()

            chunk_size = 500  #chunk row size
            print("Chunk size: "+ str(chunk_size))
            df_shirts_asin = df_shirts[["asin"]].copy()

            df_shirts_asin_chunks = [df_shirts_asin[i:i+chunk_size] for i in range(0,df_shirts_asin.shape[0],chunk_size)]
            for i, df_shirts_asin_chunk in enumerate(df_shirts_asin_chunks):
                print("Chunk %s of %s" %(i, len(df_shirts_asin_chunks)))
                asin_list = df_shirts_asin_chunk["asin"].tolist()
                if_exists = "append"
                if i == 0:
                    if_exists="replace"
                    self.df_shirts_detail_daily = bq_client.query(self.get_sql_shirts_detail_daily(marketplace,asin_list=asin_list, limit=limit)).to_dataframe().drop_duplicates()
                    #df_shirts_detail_daily["date"] = df_shirts_detail_daily.apply(lambda x: datetime.datetime.strptime(re.search(r'\d{4}-\d{2}-\d{2}', x["timestamp"]).group(), '%Y-%m-%d').date(), axis=1)
                    self.df_shirts_detail_daily["date"] = self.df_shirts_detail_daily.apply(lambda x: x["timestamp"].date(), axis=1)
                    self.df_shirts_detail_daily.to_csv("merchwatch/data/shirts_detail_daily.csv", index=None, sep="\t")
                else:
                    self.df_shirts_detail_daily = bq_client.query(self.get_sql_shirts_detail_daily(marketplace,asin_list=asin_list, limit=limit)).to_dataframe().drop_duplicates()
                    #df_shirts_detail_daily["date"] = df_shirts_detail_daily.apply(lambda x: datetime.datetime.strptime(re.search(r'\d{4}-\d{2}-\d{2}', x["timestamp"]).group(), '%Y-%m-%d').date(), axis=1)
                    self.df_shirts_detail_daily["date"] = self.df_shirts_detail_daily.apply(lambda x: x["timestamp"].date(), axis=1)
                    self.df_shirts_detail_daily.to_csv("merchwatch/data/shirts_detail_daily.csv", index=None, sep="\t", mode="a", header=False)
                
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
                df_shirts_asin_chunk.to_gbq("mba_de.plots", project_id="mba-pipeline", if_exists=if_exists)
                gc.collect()
            
            df_shirts_with_more_info = self.make_trend_column(df_shirts_with_more_info)
            try:
                df_shirts_old=pd.read_csv("merchwatch/data/shirts.csv", sep="\t")
                df_shirts_with_more_info["trend_change"] = df_shirts_with_more_info.apply(lambda x: df_shirts_old[df_shirts_old["asin"] == x["asin"]].iloc[0]["trend_nr"] - x["trend_nr"],axis=1)
            except:
                df_shirts_with_more_info["trend_change"] = 0
            # save dataframe with shirts in local storage
            df_shirts_with_more_info.to_csv("merchwatch/data/shirts.csv", index=None, sep="\t")
            df_shirts = df_shirts_with_more_info
            # make memory space free
            self.df_shirts_detail_daily = None
            print("Loading completed. Elapsed time: %.2f minutes" %((time.time() - start_time) / 60))
            #df_shirts[df_shirts["bsr_mean"] != 0][["trend", "time_since_upload","time_since_upload_norm", "bsr_mean"]].head(10)
        
        return df_shirts

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
        config = {'displayModeBar': False}#{"staticPlot": True}
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
                     "layout": go.Layout(yaxis = dict(visible=True, autorange="reversed"),  margin={'t': 0,'b': 0,'r': 0,'l': 0} )},
                output_type='div', include_plotlyjs=False, show_link=False, link_text="",image_width=400, image_height=300, config=config)
        return plot_div
'''
dataHandleModel = DataHandler()
#dataHandleModel.get_sql_shirts("de", None)
project_id = 'mba-pipeline'
bq_client = bigquery.Client(project=project_id)
marketplace = "de"
limit = None
filter=None
#df_shirts_detail_daily = bq_client.query(dataHandleModel.get_sql_shirts_detail_daily(marketplace, limit)).to_dataframe().drop_duplicates()
df_shirts = dataHandleModel.get_shirts(marketplace, limit=limit)
df_shirts2 = df_shirts.iloc[0:10].copy()
#df_shirts2["plot"] = df_shirts2.apply(lambda x: dataHandleModel.create_plot_html(x,df_shirts_detail_daily), axis=1)


#df_shirts_detail_daily["date"] = df_shirts_detail_daily.apply(lambda x: x["timestamp"].date(), axis=1)
#df_shirts2["bsr_last"], df_shirts2["price_last"], df_shirts2["bsr_first"], df_shirts2["price_first"] = df_shirts2.apply(lambda x: get_first_and_last_data(x["asin"]), axis=1)
'''
test = 0
