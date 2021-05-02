import collections
import pandas as pd
import numpy as np
from firestore_handler import Firestore
import hashlib
from scrapy.utils.python import to_bytes
import subprocess
from os.path import join
import datetime

def list_str_to_list(list_str):
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
    
class NicheUpdater():
    def __init__(self, marketplace="de", dev=False):
        dev_str = ""
        if dev:
            dev_str = "_dev"

        self.marketplace = marketplace
        self.firestore = Firestore(marketplace + "_niches" + dev_str)

    def crawl_niches(self, list_niches_str):
        shell_command = '''cd .. 
        cd ..
        cd crawler/mba/mba_crawler
        sudo /usr/bin/python3 create_url_csv.py {0} True --number_products=0  --niches="{1}"
        sudo scrapy crawl mba_general_de -a marketplace={0} -a daiy=True
        '''.format(self.marketplace, list_niches_str)
        subprocess.call(shell_command, shell=True)
        test = 0

    def get_asins_uploads_price_sql(self):
        SQL_STATEMENT = """SELECT t1.asin, t1.upload_date,CAST(REPLACE(
            t2.price,
            ',',
            '.') as FLOAT64) as price
        FROM `mba-pipeline.mba_{0}.products_details` t1
        LEFT JOIN (SELECT distinct asin, price FROM `mba-pipeline.mba_{0}.products`) t2 on t1.asin = t2.asin
        order by t1.upload_date
        """.format(self.marketplace)

        return SQL_STATEMENT

    def get_niche_data_sql(self, keywords=None):
        WHERE_STATEMENT = "and count > 5 and count < 100 and bsr_mean < 750000 and bsr_best < 200000 and price_mean > 18"
        if keywords != None and keywords != "":
            keywords_str = "({})".format(",".join(["'" + v + "'" for v in keywords.split(";")]))
            WHERE_STATEMENT = " and keyword in {}".format(keywords_str)

        SQL_STATEMENT = """
        WITH last_date as (SELECT * FROM (SELECT date, count(*) as count FROM `mba-pipeline.mba_{0}.niches` group by date order by date desc) where count > 1000 LIMIT 1) 
    SELECT t0.*
        FROM `mba-pipeline.mba_{0}.niches` t0 LEFT JOIN 
        (
    SELECT keyword
        FROM `mba-pipeline.mba_{0}.niches`
        where date = (SELECT date FROM last_date) {1}
        group by keyword) t1 on t0.keyword = t1.keyword 
        where t1.keyword IS NOT NULL
        order by date
        """.format(self.marketplace, WHERE_STATEMENT)

        return SQL_STATEMENT

    def update_firestore_niche_data(self, keywords=None):
        self.df_upload_data = pd.read_gbq(self.get_asins_uploads_price_sql(), project_id="mba-pipeline")
        self.df_upload_data["date"] = self.df_upload_data.apply(lambda x: str(x["upload_date"].date()),axis=1)
        self.df_upload_data = self.df_upload_data[self.df_upload_data["date"]!="1995-01-01"]

        df_niche_data_keywords = pd.read_gbq(self.get_niche_data_sql(keywords=keywords), project_id="mba-pipeline")
        keywords = df_niche_data_keywords.groupby(by=["keyword"]).count()["count"].index.tolist()
        df_keywords = pd.DataFrame(data={"keyword": keywords})
        df_keywords["timestamp"] = datetime.datetime.now()
        df_keywords.to_gbq("mba_" + str(self.marketplace) +".niches_firestore", project_id="mba-pipeline", if_exists="append")

        for keyword in keywords:
            df_niche_data_keyword = df_niche_data_keywords[df_niche_data_keywords["keyword"]==keyword]
            firestore_dict = self.get_firestore_dict(df_niche_data_keyword)
            keyword_guid = hashlib.sha1(to_bytes(firestore_dict["keyword"])).hexdigest()
            self.firestore.db.collection(self.firestore.collection_name).document(keyword_guid).set(firestore_dict)

    def calc_price(self, price_upload_data_cum, count_upload_data_cum, price_mean_crawling, count_crawling, count_inactive_crawling):
        count_active_total = int(count_upload_data_cum - count_inactive_crawling)
        count_active_crawling = int(count_crawling - count_inactive_crawling)
        count_active_unkown =  count_active_total - count_active_crawling
        #print(count_active_unkown, count_active_total, count_active_crawling)
        price_mean = (price_upload_data_cum * count_active_unkown/count_active_total) + (price_mean_crawling * count_active_crawling / count_active_total)
        price_mean = float("%.2f" % price_mean)
        return price_mean

    def update_takedown_dict(self, takedown_dict, takedowns_by_date, date_str):
        try:
            takedowns = sum(takedown_dict.values())
            if takedowns == takedowns_by_date:
                pass
            elif takedowns_by_date > takedowns:
                takedowns_dif = takedowns_by_date - takedowns
                takedown_dict.update({date_str: int(takedowns_dif)})
        except:
            takedown_dict[date_str] = int(takedowns_by_date)

    def update_bsr_dict(self, bsr_dict, bsr_by_date, date_str):
        try:
            bsr_last = bsr_dict[list(bsr_dict)[-1]]
            if bsr_last == bsr_by_date:
                pass
            elif bsr_by_date != bsr_last:
                if bsr_by_date != 0:
                    bsr_dict.update({date_str: int(bsr_by_date)})
        except:
            if bsr_by_date != 0:
                bsr_dict[date_str] = int(bsr_by_date)

    def get_firestore_dict(self, df_niche_data_keyword):
        df_crawling_last = df_niche_data_keyword.iloc[-1]
        firestore_dict = {}
        for columns in df_crawling_last.index.values:
            if columns not in ["asin"]:
                df_value = df_crawling_last[columns]
                if type(df_value) != str and np.issubdtype(df_value, np.floating):
                    firestore_dict.update({columns: float(df_value)})
                elif type(df_value) != str and np.issubdtype(df_value, np.integer):
                    firestore_dict.update({columns: int(df_value)})
                else:
                    firestore_dict.update({columns: df_value})
        asins = df_crawling_last["asin"].split(",")
        firestore_dict.update({"asins": asins})
        df_upload_data_keyword_count = self.df_upload_data[self.df_upload_data["asin"].isin(asins)].groupby(by=["date"]).count()["asin"]
        df_upload_data_keyword_price = self.df_upload_data[self.df_upload_data["asin"].isin(asins)].groupby(by=["date"]).mean()["price"]
        # firestore dicts
        upload_count_dict = df_upload_data_keyword_count.to_dict()
        price_dict = {}
        bsr_dict = {}
        takedown_dict = {}

        price_upload_data_by_date = 0
        count_upload_data_cum = 0

        date_list = list(collections.OrderedDict.fromkeys(sorted(df_upload_data_keyword_price.index.to_list() + df_niche_data_keyword["date"].tolist())))
        date_list_crawling = df_niche_data_keyword["date"].tolist()

        # use upload data for price updates
        date_last_upload = df_upload_data_keyword_price.index.to_list()[-1]
        date_first_upload = df_upload_data_keyword_price.index.to_list()[0]
        for date_str in date_list:            
            try:
                niche_data_date = df_niche_data_keyword[df_niche_data_keyword["date"] <= date_str].iloc[-1][["price_mean","count","count_404", "bsr_mean"]]
                price_mean_crawling = niche_data_date["price_mean"]
                count_inactive_crawling = niche_data_date["count_404"]
                count_crawling = niche_data_date["count"]
                bsr_mean = niche_data_date["bsr_mean"]
            except:
                price_mean_crawling = 0
                count_crawling = 0
                count_inactive_crawling = 0
                bsr_mean = 0
                
            # update crawling related dicts
            if date_str in date_list_crawling:
                price_dict.update({date_str: price_mean_crawling})
                # update takedowns
                self.update_takedown_dict(takedown_dict, count_inactive_crawling, date_str)
                # update bsr
                self.update_bsr_dict(bsr_dict, bsr_mean, date_str)

            # update upload related dicts
            if date_str <= date_last_upload:
                try:
                    price_upload_data = df_upload_data_keyword_price[date_str]
                except:
                    price_upload_data = price_upload_data_by_date
                #date_str = str(date.date())
                
                # case upload data given
                try:
                    upload_count_last = upload_count_dict[date_str]
                    price_upload_data_by_date = (price_upload_data_by_date * (count_upload_data_cum-count_inactive_crawling) + price_upload_data * upload_count_last)/ (count_upload_data_cum + upload_count_last - count_inactive_crawling)
                    count_upload_data_cum = count_upload_data_cum + upload_count_last
                    price_mean = self.calc_price(price_upload_data_by_date, count_upload_data_cum, price_mean_crawling, count_crawling, count_inactive_crawling)
                    price_dict.update({date_str: price_mean})
                    #print("Price update with upload data", date_str, price_mean, price_upload_data_by_date, count_upload_data_cum, price_mean_crawling, count_crawling, count_inactive_crawling)
                # case only crawling data given
                except:
                    #price_upload_data_by_date = (price_upload_data_by_date * (count_upload_data_cum-count_inactive_crawling) + price_mean_crawling * upload_count_last)/ (count_upload_data_cum + upload_count_last - count_inactive_crawling)
                    #price_mean = float("%.2f" % price_mean)
                    price_mean = self.calc_price(price_upload_data_by_date, count_upload_data_cum, price_mean_crawling, count_crawling, count_inactive_crawling)
                    price_dict.update({date_str: price_mean})
                    #print("Price update without upload data", date_str, price_mean, price_upload_data_by_date)
                
        price_dict = collections.OrderedDict(sorted(price_dict.items()))

        firestore_dict.update({"date_last_upload": date_last_upload, "date_first_upload": date_first_upload, "uploads": upload_count_dict, "prices": price_dict, "bsr": bsr_dict, "takedowns":takedown_dict})
        
        return firestore_dict

import re
from nltk import ngrams
from shirt_handler import MerchwatchShirt
from text_rank import TextRank4Keyword
import difflib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN

KEYWORDS_TO_REMOVE_DE = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "Geschenk", "Geschenkidee", "Design", "Weihnachten", "Frau",
        "Geburtstag", "Freunde", "Sohn", "Tochter", "Vater", "Geburtstagsgeschenk", "Herren", "Frauen", "Mutter", "Schwester", "Bruder", "Kinder", 
        "Spruch", "Fans", "Party", "Geburtstagsparty", "Familie", "Opa", "Oma", "Liebhaber", "Freundin", "Freund", "Jungen", "Mädchen", "Outfit",
        "Motiv", "Damen", "Mann", "Papa", "Mama", "Onkel", "Tante", "Nichte", "Neffe", "Jungs", "gift", "Marke", "Kind", "Anlass", "Jubiläum"
        , "Überraschung"]

KEYWORDS_TO_REMOVE_EN = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "gift", "Brand", "family", "children", "friends", "sister", "brother",
    "childreen", "present", "boys", "girls"]

# function that converts tuple to string
def join_tuple_string(strings_tuple) -> str:
   return ' '.join(strings_tuple)

class NicheAnalyser():
    
    def __init__(self, marketplace="de", dev=False, project="mba-pipeline"):
        dev_str = ""
        if dev:
            dev_str = "_dev"

        self.marketplace = marketplace
        self.project = project
        self.bq_table_id = f"{project}.mba_{marketplace}.merchwatch_shirts{dev_str}"
        self.trademarks = pd.read_gbq(f"SELECT brand FROM `{project}.mba_{marketplace}.products_trademark`", project_id=project)["brand"].to_list()
        self.df = pd.DataFrame()
        if self.marketplace == "de":
            self.banned_words = [keyword.lower() for keyword in KEYWORDS_TO_REMOVE_DE]
        elif self.marketplace == "com":
            self.banned_words = [keyword.lower() for keyword in KEYWORDS_TO_REMOVE_EN]
        else:
            raise ValueError("Marketplace not known")
        self.banned_words = self.banned_words + ["t"]
        self.tr4w_de = TextRank4Keyword(language="de")
        self.tr4w_en = TextRank4Keyword(language="en")
        
    def get_raw_design_data_sql(self, limit=1000):
        SQL_STATEMENT = """
        SELECT price_last, asin, title, brand, product_features, upload_date FROM `{0}` 
        where not takedown and title IS NOT NULL
        order by trend_nr 
        LIMIT {1}
        """.format(self.bq_table_id, limit)
        return SQL_STATEMENT

    def set_df(self):
        SQL_STATEMENT = self.get_raw_design_data_sql()
        self.df = pd.read_gbq(SQL_STATEMENT, project_id=self.project)

    def extract_keywords(self, df_row):
        print(df_row.name)
        keywords_list = []
        try:
            title = df_row["title"]
            brand = df_row["brand"]
            if brand in self.trademarks:
                print("Found trademark", brand)
                return []

            # extract unbanned keywords
            keywords_text = title + ". " + brand
            keywords = re.findall(r'\w+', keywords_text)
            keywords = [k for k in keywords if k.lower() not in self.banned_words]


            n = 2
            n_grams = ngrams(keywords, n)
            for grams in n_grams:
                print(join_tuple_string(grams))
                keywords_list = keywords_list + [join_tuple_string(grams)]

            return list(set(keywords_list))
        except Exception as e:
            print(str(e))
        return list(set(keywords_list))

    def extract_keywords_with_textrank(self, df_row):
        print(df_row.name)
        MerchwatchShirtModel = MerchwatchShirt(self.marketplace)
        MerchwatchShirtModel.load_by_dict(df_row.to_dict())
        if MerchwatchShirtModel.language == "en":
            MerchwatchShirtModel.set_keywords(self.tr4w_en)
        else:
            MerchwatchShirtModel.set_keywords(self.tr4w_de)
        keywords = MerchwatchShirtModel.get_keywords()
        #self.df.loc[df_row.name, "keywords"] = keywords
        MerchwatchShirtModel.set_stem_keywords()
        stem_keywords = MerchwatchShirtModel.get_stem_keywords() 
        #self.df.loc[df_row.name, "stem_keywords"] = stem_keywords
        return list(set(keywords)), stem_keywords  

    def jaccard_similarity(self, list1, list2):
        intersection = len(list(set(list1).intersection(list2)))
        union = (len(list1) + len(list2)) - intersection
        return float(intersection) / union
    
    def is_cluster_unique_enough(self, keyword_asins, threshold=0.5):
        for asin_cluster in self.asin_clusters:
            js = self.jaccard_similarity(asin_cluster, keyword_asins)
            if js > threshold:
                return False
        return True

    def set_keyword_cluster(self):
        labellist = self.df["asin"].to_list()
        textlist = [" ".join(stem_keyword) for stem_keyword in self.df["stem_keywords"].to_list()]
        LANGUAGE = 'english' # used for snowball stemmer
        SENSITIVITY = 0.2 # The Lower the more clusters
        MIN_CLUSTERSIZE = 2
        tfidf_vectorizer = TfidfVectorizer(max_df=0.2, max_features=10000,min_df=0.01,use_idf=True, ngram_range=(1,2))
        tfidf_matrix = tfidf_vectorizer.fit_transform(textlist)
        ds = DBSCAN(eps=SENSITIVITY, min_samples=MIN_CLUSTERSIZE).fit(tfidf_matrix)
        clusters = ds.labels_.tolist()
            
        cluster_df = pd.DataFrame(clusters, columns=['Cluster'])
        self.df = pd.merge(cluster_df, self.df, left_index=True, right_index=True)
        #keywords_df =  pd.DataFrame(labellist, columns=['Keyword'])
        #result = pd.merge(cluster_df, keywords_df, left_index=True, right_index=True)
        #grouping = result.groupby(['Cluster'])['Keyword'].apply(' | '.join).reset_index()
        #grouping.to_csv("clustered_queries.csv",index=False)

    def analyze(self):
        df_row = self.df.iloc[93]
        MerchwatchShirtModel = MerchwatchShirt(self.marketplace)
        MerchwatchShirtModel.load_by_dict(df_row.to_dict())
        MerchwatchShirtModel.set_keywords(self.tr4w_de)
        keywords = MerchwatchShirtModel.get_keywords()
        series_keyword_tuple = self.df.apply(lambda x: self.extract_keywords_with_textrank(x), axis=1)
        df_keywords = pd.DataFrame(series_keyword_tuple.tolist(),index=series_keyword_tuple.index)
        self.df["keywords"] = df_keywords.iloc[:,0]
        self.df["stem_keywords"] = df_keywords.iloc[:,1]
        keywords_dict = {}
        for i, df_row in self.df.iterrows():
            keywords = df_row["keywords"]
            for keyword in keywords:
                if len(keyword.split(" "))>1:
                    if keyword in keywords_dict:
                        keywords_dict[keyword] = keywords_dict[keyword] + 1
                    else:
                        keywords_dict[keyword] = 1
        keywords_dict_sorted = {k: v for k, v in sorted(keywords_dict.items(), key=lambda item: item[1], reverse=True)}
        self.asin_clusters = []
        for keyword, count in keywords_dict_sorted.items():
            if count > 20:
                # TODO: keyword is wrong and not the one from iteration
                # keyword in x
                mask = self.df.keywords.apply(lambda x: print(keyword))
                keyword_asins = self.df[mask].asin.tolist()
                print(keyword_asins, keyword)
                if self.is_cluster_unique_enough(keyword_asins, threshold=0.5):
                    self.asin_clusters.append(keyword_asins)

        test = 0
    
