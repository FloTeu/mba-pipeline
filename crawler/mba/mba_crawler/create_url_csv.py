from bs4 import BeautifulSoup
import requests 
import pandas as pd
import numpy as np
import argparse
import sys
import random
from google.cloud import bigquery
import datetime
from pathlib import Path


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def does_table_exist(project_id, dataset_id, table_id):
    client = bigquery.Client()
    try:
        df = client.query("SELECT * FROM %s.%s.%s" %(project_id, dataset_id, table_id)).to_dataframe().drop_duplicates()
        return True
    except Exception as e:
        return False

def get_asin_product_detail_crawled(marketplace):
    project_id = 'mba-pipeline'
    reservationdate = datetime.datetime.now()
    dataset_id = "preemptible_logs"
    table_id = "mba_detail_" + marketplace + "_preemptible_%s_%s_%s" % (
    reservationdate.year, reservationdate.month, reservationdate.day)
    reservation_table_id = dataset_id + "." + table_id
    bq_client = bigquery.Client(project=project_id)
    df_product_details = bq_client.query(
        "SELECT t0.asin, t0.url_product FROM mba_" + marketplace + ".products t0 LEFT JOIN mba_" + marketplace + ".products_details t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp").to_dataframe().drop_duplicates(["asin"])
    if does_table_exist(project_id, dataset_id, table_id):
        # get reservation logs
        df_reservation = bq_client.query(
            "SELECT * FROM " + reservation_table_id + " t0 order by t0.timestamp DESC").to_dataframe().drop_duplicates(["asin"])
        df_reservation_status = df_reservation.drop_duplicates("asin")
        # get list of asins that are currently blocked by preemptible instances
        asins_blocked = df_reservation_status[df_reservation_status["status"] == "blocked"]["asin"].tolist()
        # filter asins for those which are not blocked
        matching_asins = df_product_details["asin"].isin(asins_blocked)
        print("%s asins are currently blocked and will not be crawled" % str(
            len([i for i in matching_asins if i == True])))
        df_product_details = df_product_details[~matching_asins]

    return df_product_details

def get_asin_product_detail_daily_crawled(marketplace):
    project_id = 'mba-pipeline'
    reservationdate = datetime.datetime.now()
    dataset_id = "preemptible_logs"
    table_id = "mba_detail_daily_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day)
    reservation_table_id = dataset_id + "." + table_id
    bq_client = bigquery.Client(project=project_id)
    # TODO get those which are not already crawled today
    # TODO remove asins that return a 404 (not found) error
    SQL_STATEMENT = '''
    SELECT t0.asin, t0.url_product, t2.bsr_count FROM mba_{0}.products t0 LEFT JOIN (SELECT * FROM mba_{0}.products_details_daily WHERE DATE(timestamp) = '{1}-{2}-{3}' or price_str = '404') t1 on t0.asin = t1.asin 
        LEFT JOIN 
        (
        SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
                    AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min, COUNT(*) as bsr_count,
                    AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
                    FROM `mba-pipeline.mba_{0}.products_details_daily`
            where bsr != 404
            group by asin
        ) t2 on t0.asin = t2.asin 
        where t1.asin IS NULL 
        order by t2.bsr_count
    '''.format(marketplace, reservationdate.year, reservationdate.month, reservationdate.day)
    #df_product_details = bq_client.query("SELECT t0.asin, t0.url_product FROM mba_" + marketplace + ".products t0 LEFT JOIN (SELECT * FROM mba_" + marketplace + ".products_details_daily WHERE DATE(timestamp) = '%s-%s-%s' or price_str = '404') t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp" %(reservationdate.year, reservationdate.month, reservationdate.day)).to_dataframe().drop_duplicates()
    df_product_details = bq_client.query(SQL_STATEMENT).to_dataframe().drop_duplicates(["asin"])

    if does_table_exist(project_id, dataset_id, table_id):
        # get reservation logs
        df_reservation = bq_client.query("SELECT * FROM " + reservation_table_id + " t0 order by t0.timestamp DESC").to_dataframe().drop_duplicates(["asin"])
        df_reservation_status = df_reservation.drop_duplicates("asin")
        # get list of asins that are currently blocked by preemptible instances
        asins_blocked = df_reservation_status[df_reservation_status["status"] == "blocked"]["asin"].tolist()
        # filter asins for those which are not blocked
        matching_asins = df_product_details["asin"].isin(asins_blocked)
        print("%s asins are currently blocked and will not be crawled" % str(len([i for i in matching_asins if i == True])))
        df_product_details = df_product_details[~matching_asins]
    
    return df_product_details

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('daily', type=str2bool, nargs='?', const=True, help='Should the webcrawler for daily crawling be used or the normal one time detail crawler?')
    parser.add_argument('--number_products', default=10, type=int, help='Number of products/shirts that shoul be crawled. If 0, every image that is not already crawled will be crawled.')
    parser.add_argument('--proportion_priority_low_bsr_count', default=0.5, type=float, help='50% is the default proportion what means 50% should be design which were crawled least often')

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    daily = args.daily
    number_products = args.number_products
    proportion_priority_low_bsr_count = args.proportion_priority_low_bsr_count

    filename = "urls"
    if daily:
        # get asins which are not already crawled
        df_product_details_tocrawl_total = get_asin_product_detail_daily_crawled(marketplace)
        df_product_details_tocrawl = df_product_details_tocrawl_total[0:int(number_products*proportion_priority_low_bsr_count)].reset_index(drop=True)
        # remove asins that are in priority order (df_product_details_tocrawl)
        df_product_details_tocrawl_total = df_product_details_tocrawl_total[~df_product_details_tocrawl_total.asin.isin(df_product_details_tocrawl["asin"].tolist())]
        # insert random variables
        df_product_details_tocrawl = df_product_details_tocrawl.append(df_product_details_tocrawl_total.sample(frac=1).reset_index(drop=True))
        filename = "urls_mba_daily_" + marketplace
    else:
        # get asins which are not already crawled generally
        df_product_details_tocrawl = get_asin_product_detail_crawled(marketplace)
        filename = "urls_mba_general_" + marketplace

    #df_product_details = pd.DataFrame(data={"asin": ["B07RVNJHZL"], "url_product": ["adwwadwad"]})
    try:
        df_product_details_tocrawl["url"] = df_product_details_tocrawl.apply(lambda x: "https://www.amazon."+marketplace+"/dp/"+x["asin"], axis=1)
    except:
        df_product_details_tocrawl["url"] = []

    # if number_images is equal to -1, every image should be crawled
    if number_products == -1:
        number_products = len(df_product_details_tocrawl)

    Path("mba_crawler/url_data/").mkdir(parents=True, exist_ok=True)
    df_product_details_tocrawl[["url", "asin"]].iloc[0:number_products].to_csv("mba_crawler/url_data/" + filename + ".csv",index=False)

if __name__ == '__main__':
    main(sys.argv)

