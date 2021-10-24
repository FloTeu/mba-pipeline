
import pandas as pd
import numpy as np
import argparse
import sys
import random
from google.cloud import bigquery
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, validator, Field
from typing import List, Optional
from contextlib import suppress
from google.api_core.exceptions import NotFound

from mwfunctions.pydantic.crawling_classes import CrawlingMBAProductRequest, CrawlingInputItem
from mwfunctions.crawler.preprocessing.excluded_asins import EXCLUDED_ASINS, STRANGE_LAYOUT
from mwfunctions.io import str2bool


def does_table_exist(project_id, dataset_id, table_id):
    client = bigquery.Client()
    try:
        df = client.query("SELECT * FROM %s.%s.%s" %(project_id, dataset_id, table_id)).to_dataframe().drop_duplicates()
        return True
    except Exception as e:
        return False

def get_asin_product_detail_crawled(marketplace):
    project_id = 'mba-pipeline'
    reservationdate = datetime.now()
    dataset_id = "preemptible_logs"
    table_id = "mba_detail_" + marketplace + "_preemptible_%s_%s_%s" % (
    reservationdate.year, reservationdate.month, reservationdate.day)
    reservation_table_id = dataset_id + "." + table_id
    bq_client = bigquery.Client(project=project_id)
    df_product_details = bq_client.query(
        "SELECT t0.asin, t0.url_product FROM mba_" + marketplace + ".products t0 LEFT JOIN mba_" + marketplace + ".products_details t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp").to_dataframe().drop_duplicates(["asin"])
    # code is old and from deprecated crawling worklfow
    # if does_table_exist(project_id, dataset_id, table_id):
    #     # get reservation logs
    #     df_reservation = bq_client.query(
    #         "SELECT * FROM " + reservation_table_id + " t0 order by t0.timestamp DESC").to_dataframe().drop_duplicates(["asin"])
    #     df_reservation_status = df_reservation.drop_duplicates("asin")
    #     # get list of asins that are currently blocked by preemptible instances
    #     asins_blocked = df_reservation_status[df_reservation_status["status"] == "blocked"]["asin"].tolist()
    #     # filter asins for those which are not blocked
    #     matching_asins = df_product_details["asin"].isin(asins_blocked)
    #     print("%s asins are currently blocked and will not be crawled" % str(
    #         len([i for i in matching_asins if i == True])))
    #     df_product_details = df_product_details[~matching_asins]

    return df_product_details

def get_sql_exclude_asins(marketplace):
    today = datetime.now()
    SQL_STATEMENT = '''
    SELECT asin FROM mba_{0}.products_details_daily WHERE DATE(timestamp) = '{1}-{2}-{3}' or price_str = '404'
    '''.format(marketplace, today.year, today.month, today.day)
    return SQL_STATEMENT

def get_sql_exclude_asins_api(marketplace):
    today = datetime.now()
    SQL_STATEMENT = '''
    SELECT asin FROM mba_{0}.products_details_daily_api WHERE DATE(timestamp) = '{1}-{2}-{3}' or price = 404
    '''.format(marketplace, today.year, today.month, today.day)
    return SQL_STATEMENT

def get_sql_products_no_bsr(marketplace):
    SQL_STATEMENT = '''
    SELECT DISTINCT asin FROM mba_{0}.products_no_bsr
    '''.format(marketplace)
    return SQL_STATEMENT

def get_sql_products_no_mba_shirt(marketplace):
    # urls which have no rigth url, i.e. amazon.{}/dp/ could not be crawled correctly and therefore should not be blacklisted
    SQL_STATEMENT = '''
    SELECT DISTINCT asin FROM mba_{0}.products_no_mba_shirt where url LIKE '%amazon.{0}/dp/%'
    '''.format(marketplace)
    return SQL_STATEMENT

def get_sql_best_seller(marketplace):
    SQL_STATEMENT = '''
    SELECT asin FROM  `mba-pipeline.mba_{0}.products_mba_relevance` 
        WHERE EXTRACT(DATE FROM timestamp) = (SELECT EXTRACT(DATE FROM timestamp) as date FROM `mba-pipeline.mba_{0}.products_mba_relevance` where sort = 'best_seller' group by date order by date desc LIMIT 1)
        and sort = 'best_seller'
        order by number
    '''.format(marketplace)
    return SQL_STATEMENT

def get_sql_watchlist(marketplace):
    SQL_STATEMENT = '''
    SELECT asin, operation FROM  `mba_{0}.watchlist` 
    order by timestamp desc
    '''.format(marketplace)
    return SQL_STATEMENT

def get_sql_lowest_bsr_count(marketplace):
    SQL_STATEMENT = '''
    SELECT t0.asin, t2.bsr_count FROM mba_{0}.products t0 
        LEFT JOIN 
        (
        SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
                    AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min, COUNT(*) as bsr_count,
                    AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
                    FROM `mba-pipeline.mba_{0}.products_details_daily`
            where bsr != 404
            group by asin
        ) t2 on t0.asin = t2.asin 
        order by t2.bsr_count
    '''.format(marketplace)
    return SQL_STATEMENT

def get_sql_random(marketplace, number_products):
    SQL_STATEMENT = '''
    SELECT asin FROM mba_{0}.products
    WHERE RAND() < {1}/(SELECT COUNT(*) FROM `mba_{0}.products`)
    '''.format(marketplace, number_products)
    return SQL_STATEMENT

def get_sql_top_categories(marketplace, top_n=10):
    SQL_STATEMENT = '''
    (SELECT DISTINCT asin, bsr_last, bsr_mean, trend_nr, bsr_change FROM `mba_{0}.merchwatch_shirts` where price_last != 404.0 order by bsr_last LIMIT {1})
    UNION ALL
    (SELECT DISTINCT asin, bsr_last, bsr_mean, trend_nr, bsr_change FROM `mba_{0}.merchwatch_shirts` where price_last != 404.0 order by bsr_mean LIMIT {1})
    UNION ALL
    (SELECT DISTINCT asin, bsr_last, bsr_mean, trend_nr, bsr_change FROM `mba_{0}.merchwatch_shirts` where price_last != 404.0 order by trend_nr LIMIT {1})
    UNION ALL
    (SELECT DISTINCT asin, bsr_last, bsr_mean, trend_nr, bsr_change FROM `mba_{0}.merchwatch_shirts` where price_last != 404.0 order by bsr_change LIMIT {1})
    '''.format(marketplace, top_n)
    return SQL_STATEMENT

def get_crawling_input_items(mba_product_request: CrawlingMBAProductRequest, bq_project_id="mba-pipeline") -> List[CrawlingInputItem]:
    asin_list = get_asins_to_crawl(mba_product_request, bq_project_id)
    return [CrawlingInputItem(asin=asin, marketplace=mba_product_request.marketplace) for asin in asin_list]

def get_asins_to_crawl(mba_product_request: CrawlingMBAProductRequest, bq_project_id="mba-pipeline") -> list:
    if mba_product_request.daily:
        return get_asins_daily_to_crawl(mba_product_request, bq_project_id)
    else:
        bq_client = bigquery.Client(project=bq_project_id)
        try:
            df_product_details = bq_client.query(
                "SELECT t0.asin, t0.url_product FROM mba_" + mba_product_request.marketplace + ".products t0 LEFT JOIN mba_" + mba_product_request.marketplace + ".products_details t1 on t0.asin = t1.asin where t1.asin IS NULL order by t0.timestamp").to_dataframe().drop_duplicates(
                ["asin"])
        except NotFound: # case products_details does not exist
            df_product_details = bq_client.query(
                "SELECT t0.asin, t0.url_product FROM mba_" + mba_product_request.marketplace + ".products t0 order by t0.timestamp").to_dataframe().drop_duplicates(
                ["asin"])
        return df_product_details["asin"].to_list()
        # raise NotImplementedError("daily = False crawling asins are not defined at the moment")


def get_asins_daily_to_crawl(mba_product_request: CrawlingMBAProductRequest, bq_project_id="mba-pipeline") -> list:# marketplace, exclude_asins, number_products, top_n=60, proportions=[0.7,0.2,0.1]):
    '''
        Logic of daily crawling:
        70% random pick of best sellers (Last crawled date in table products_mba_relevance)
        20% lowest bsr count
        10% random 
        4 * top n best_seller + trend + bsr_change + bsr_last
    '''
    df_best_seller = pd.DataFrame(columns=["asin"])
    df_lowest_bsr_count = pd.DataFrame(columns=["asin"])
    df_random = pd.DataFrame(columns=["asin"])

    # get asins which should be excluded
    exclude_asins = mba_product_request.excluded_asins
    with suppress(Exception):
        pd.read_gbq(get_sql_exclude_asins(mba_product_request.marketplace), project_id=bq_project_id)["asin"].to_list()
    with suppress(Exception):
        exclude_asins = exclude_asins + pd.read_gbq(get_sql_exclude_asins_api(mba_product_request.marketplace), project_id=bq_project_id)["asin"].to_list()

    # exclude asins with no bsr information
    with suppress(Exception):
        exclude_asins = exclude_asins + pd.read_gbq(get_sql_products_no_bsr(mba_product_request.marketplace), project_id=bq_project_id)["asin"].to_list()

    # exclude asins with no mba shirt
    with suppress(Exception):
        exclude_asins = exclude_asins + pd.read_gbq(get_sql_products_no_mba_shirt(mba_product_request.marketplace), project_id=bq_project_id)["asin"].to_list()

    # get 70% random best seller
    number_best_sellers = int(int(mba_product_request.number_products) * mba_product_request.proportions.best_seller)
    with suppress(Exception):
        df_best_seller = pd.read_gbq(get_sql_best_seller(mba_product_request.marketplace), project_id=bq_project_id)
        df_best_seller = df_best_seller[~df_best_seller['asin'].isin(exclude_asins)]
        if df_best_seller.shape[0] > number_best_sellers:
            df_best_seller = df_best_seller.sample(number_best_sellers)

    # update exclude_asins
    exclude_asins = exclude_asins + df_best_seller["asin"].to_list()

    # get 20% lowest bsr_count
    with suppress(Exception):
        number_lowest_bsr_count = int(int(mba_product_request.number_products) * mba_product_request.proportions.lowest_bsr_count)
        df_lowest_bsr_count = pd.read_gbq(get_sql_lowest_bsr_count(mba_product_request.marketplace), project_id=bq_project_id)
        df_lowest_bsr_count = df_lowest_bsr_count[~df_lowest_bsr_count['asin'].isin(exclude_asins)]
        if df_lowest_bsr_count.shape[0] > number_lowest_bsr_count:
            df_lowest_bsr_count = df_lowest_bsr_count.iloc[0:number_lowest_bsr_count]

    # update exclude_asins
    exclude_asins = exclude_asins + df_lowest_bsr_count["asin"].to_list()

    # get 10 % random
    with suppress(Exception):
        number_random_count = int(int(mba_product_request.number_products) * mba_product_request.proportions.random)
        # get two times more to filter alreay existent asins later
        df_random = pd.read_gbq(get_sql_random(mba_product_request.marketplace, number_random_count*4), project_id=bq_project_id)
        df_random = df_random[~df_random['asin'].isin(exclude_asins)]
        if df_random.shape[0] > number_random_count:
            df_random = df_random.sample(number_random_count)


    # update exclude_asins
    exclude_asins = exclude_asins + df_random["asin"].to_list()

    # get 40 top 10 best_seller + trend + bsr_change + bsr_last
    try:
        df_ranking = pd.read_gbq(get_sql_top_categories(mba_product_request.marketplace, top_n=mba_product_request.top_n), project_id=bq_project_id)
    except:
        df_ranking = df_random.iloc[0:2]
    df_ranking = df_ranking[~df_ranking['asin'].isin(exclude_asins)]

    # get watchlist data
    try:
        df_watchlist = pd.read_gbq(get_sql_watchlist(mba_product_request.marketplace), project_id=bq_project_id)
        df_watchlist = df_watchlist.drop_duplicates(subset=['asin'], keep='first')
        df_watchlist = df_watchlist[df_watchlist["operation"] == "insert"]
    except:
        df_watchlist = df_random.iloc[0:2]
    df_watchlist = df_watchlist[~df_watchlist['asin'].isin(exclude_asins)]
    # allow only a maximum of 50 watchlist asins per crawling
    df_watchlist = df_watchlist.iloc[0:50]

    pd_list = [df_best_seller[["asin"]], df_lowest_bsr_count[["asin"]], df_random[["asin"]], df_ranking[["asin"]], df_watchlist[["asin"]]]
    df_total = pd.concat(pd_list).drop_duplicates(["asin"])
    # return shuffled
    return df_total.sample(len(df_total))["asin"].tolist()


def get_asin_product_detail_daily_crawled(marketplace):

    project_id = 'mba-pipeline'
    reservationdate = datetime.now()
    dataset_id = "preemptible_logs"
    table_id = "mba_detail_daily_" + marketplace + "_preemptible_%s_%s_%s"%(reservationdate.year, reservationdate.month, reservationdate.day)
    reservation_table_id = dataset_id + "." + table_id
    bq_client = bigquery.Client(project=project_id)
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

def get_sql_asins_by_niches(marketplace, list_niches):
    list_niches_str = "'" + "','".join(list_niches) + "'"
    SQL_STATEMENT = """DECLARE last_date STRING DEFAULT (SELECT date FROM (SELECT date, count(*) as count FROM `mba-pipeline.mba_{0}.niches` group by date order by date desc) where count > 1000 LIMIT 1) ;

    WITH keywords as (SELECT *
    FROM UNNEST([{1}])
    AS keyword
    WITH OFFSET AS offset
    ORDER BY offset)

    SELECT asin FROM `mba-pipeline.mba_{0}.niches` t0 
    LEFT JOIN keywords t1 on t0.keyword = t1.keyword
    WHERE t1.keyword IS NOT NULL
    and date = last_date
    """.format(marketplace, list_niches_str)
    
    # QUICKFIX if niches is not up to date or not used anymore
    SQL_STATEMENT = """SELECT * FROM `mba-pipeline.mba_de.products_details` where 
    """.format(marketplace, list_niches[0])
    for i, niche in enumerate(list_niches):
        niche_lower = niche.lower()
        if i != 0:
            SQL_STATEMENT = SQL_STATEMENT + " or "
        SQL_STATEMENT = SQL_STATEMENT + f" lower(product_features) LIKE '%{niche_lower}%' or lower(title) LIKE '%{niche_lower}%' or lower(brand) LIKE '%{niche_lower}%'"
    print(SQL_STATEMENT)
    return SQL_STATEMENT

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('daily', type=str2bool, nargs='?', const=True, help='Should the webcrawler for daily crawling be used or the normal one time detail crawler?')
    parser.add_argument('--number_products', default=10, type=int, help='Number of products/shirts that shoul be crawled. If -1, every image that is not already crawled will be crawled.')
    parser.add_argument('--proportion_priority_low_bsr_count', default=0.5, type=float, help='50% is the default proportion what means 50% should be design which were crawled least often')
    parser.add_argument('--niches', default="", type=str, help='multiple niches. If set only this niches are crawled')

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    daily = args.daily
    number_products = args.number_products
    proportion_priority_low_bsr_count = args.proportion_priority_low_bsr_count
    niches = args.niches
    project_id = 'mba-pipeline'

    # exclude_asins = ["B00N3THBE8", "B076LTLG1Q", "B001EAQB12", "B001EAQB12", "B00OLG9GOK", "B07VPQHZHZ", "B076LX1H2V",
    #  "B0097B9SKQ", "B001EAQBH6", "B084X5Z1RX", "B07VPQHZHZ", "B07N4CHR77", "B002LBVRJO", "B00O1QQNGE",
    #   "B084ZRCLBD", "B084JBK66T", "B07VRY4WL3", "B078KR341N", "B00MP1PPHK", "B000YEVF4C", "B07WL5C9G9"
    #   ,"B07WVM8QBX", "B076LTN3ZV", "B016QM4XAI", "B007VATVL6", "B00U6U8GXC", "B00JZQHZ6C", "B00B69A928", "B0731RSZ8V"
    #   , "B01N2I5UO7", "B01MU11HZ4", "B00K5R9XCY", "B07BP9MDDR", "B0845C7JWN", "B0731RB39G", "B00Q4L52EI", "B0731R9KN4",
    #   "B084ZRG8T8", "B07W7F64J1", "B084WYWVDY", "B00PK2JBIA", "B07G5JXZZZ", "B07MVM8QBX", "B08P45JK6P", "B08P49MY6P", "B07G57GSW3",
    #   "B07SPXP8G4", "B00N3THB8E", "B01LZ3CICA", "B07V5P1VCP", "B0731RGXDP", "B076LWZHPC", "B0731T51WL", "B073D183X3",
    #   "B07NQ41MLR", "B0719BMPLY", "B083QNVF1P", "B076LX7HR2", "B083QNKLY5", "B083QNX4RM", "B07RJRXRPZ", "B07G5HX57H",
    #   "B07G57MJHF", "B0779HF6W1", "B002LBVQS6", "B014N6DPJY", "B003Q6CM8I", "B07VCTKYLH", "B07YZB46DM", "B0731RY1SM",
    #   "B08CJJ612P", "B08CCXZ62B"]
    # strange_layout = ["B08P4P6NW2", "B08P9RSFPB", "B08P715HSQ", "B08P6ZZZYD", "B08NPN1BSM", "B08P6W8DF5", "B08P6Z741L", "B08NF2KRVD",
    # "B08P6YR7H1", "B08P745NZF", "B08P11VQT1", "B08P7254PL", "B08P6Y478X", "B08P4WF7BJ", "B08P4W854L", "B08P5WJN16", "B08P5BLGCG", "B08PB5H8MX",
    # "B08P9TJT15", "B08P96596Z", "B08P7DN9DK","B08P6S9BFW", "B08P6L9YNY", "B08P6Z6398", "B08P9HGNV1", "B08P94XH62", "B08P9T4DPT"
    # , "B08P761ZZ7", "B08P72GHH8", "B08PBPR798", "B08PBHYMTT", "B08NJMYW38", "B07X9H69QR","B08PGXQHHB", "B08PFJ28B3", "B08PGRSQKT",
    # "B08PM69M79", "B08PGX58MF", "B08PGL55LR"]
    # exclude_asins = exclude_asins + strange_layout
    exclude_asins = EXCLUDED_ASINS + STRANGE_LAYOUT
    
    # exclude asins with no mba shirt
    try:
        exclude_asins = exclude_asins + pd.read_gbq(get_sql_products_no_mba_shirt(marketplace), project_id=project_id)["asin"].to_list()
    except:
        pass

    filename = "urls"
    if niches != "":
        niches = [v.strip() for v in niches.split(";")]
        df_asin_by_niches = pd.read_gbq(get_sql_asins_by_niches(marketplace, niches), project_id=project_id)
        asin_list = []
        for i, df_row in df_asin_by_niches.iterrows():
            asins = df_row["asin"].split(",")
            asin_list.extend(asins)
        # drop duplicates
        asin_list = list(dict.fromkeys(asin_list))
        # exclude asins which are already crawled today
        exclude_asins = exclude_asins + pd.read_gbq(get_sql_exclude_asins(marketplace), project_id=project_id)["asin"].to_list()

        df_product_details_tocrawl = pd.DataFrame({"asin": asin_list})
        number_products = len(df_product_details_tocrawl)
        filename = "urls_mba_daily_" + marketplace
    elif daily:
        # get asins which are not already crawled
        #df_product_details_tocrawl_total = get_asin_product_detail_daily_crawled(marketplace)
        #df_product_details_tocrawl = df_product_details_tocrawl_total[0:int(number_products*proportion_priority_low_bsr_count)].reset_index(drop=True)
        # remove asins that are in priority order (df_product_details_tocrawl)
        #df_product_details_tocrawl_total = df_product_details_tocrawl_total[~df_product_details_tocrawl_total.asin.isin(df_product_details_tocrawl["asin"].tolist())]
        # insert random variables
        #df_product_details_tocrawl = df_product_details_tocrawl.append(df_product_details_tocrawl_total.sample(frac=1).reset_index(drop=True))
        df_product_details_tocrawl = get_asins_daily_to_crawl(marketplace, exclude_asins, number_products)
        number_products = len(df_product_details_tocrawl)
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

    # reset index
    df_product_details_tocrawl = df_product_details_tocrawl.reset_index(drop=True)

    # drop asins to exclude
    df_product_details_tocrawl = df_product_details_tocrawl[~df_product_details_tocrawl['asin'].isin(exclude_asins)]

    # if number_images is equal to -1, every image should be crawled
    if number_products == -1:
        number_products = len(df_product_details_tocrawl)

    Path("mba_crawler/url_data/").mkdir(parents=True, exist_ok=True)
    print(str(len(df_product_details_tocrawl.iloc[0:number_products])) + " number of products stored in csv")
    df_product_details_tocrawl[["url", "asin"]].iloc[0:number_products].to_csv("mba_crawler/url_data/" + filename + ".csv",index=False)

if __name__ == '__main__':
    main(sys.argv)

