from google.cloud import bigquery

from bigquery_handler import BigqueryHandler

import pandas as pd
import dask.dataframe as dd
import numpy as np
import time
import argparse
import sys
import gc
import re
from sklearn import preprocessing
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool 
from functools import partial


ADDITIONAL_DATA_COLS = ["bsr_last", "price_last", "bsr_first", "price_first", "bsr_change", "bsr_change_total", "price_change", "update_last", "score_last", "score_count", "bsr_count", "bsr_category"]

def get_sql_shirts(marketplace, limit=None, filter=None):
    if limit == None:
        SQL_LIMIT = ""
    elif type(limit) == int and limit > 0:
        SQL_LIMIT = "LIMIT " + str(limit)
    else:
        assert False, "limit is not correctly set"

    if filter == None:
        SQL_WHERE = "where bsr != 0 and bsr != 404"
    elif filter == "only 404":
        SQL_WHERE = "where bsr = 404"
    elif filter == "only 0":
        SQL_WHERE = "where bsr = 0"
    else:
        assert False, "filter is not correctly set"

    SQL_STATEMENT = """
    SELECT t_fin.* FROM (
SELECT t0.*, t2.title, t2.brand, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t3.url_image_hq as url_mba_hq, t3.url_image_lowq as url_mba_lowq, t3.url_image_q2, t3.url_image_q3, t3.url_image_q4 FROM (
    SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
            AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min,
            AVG(customer_review_score_mean) as score_mean, MAX(customer_review_score_mean) as score_max, MIN(customer_review_score_mean) as score_min 
            FROM `mba-pipeline.mba_{0}.products_details_daily`
    where bsr != 0 and bsr != 404
    group by asin
    ) t0
    left join `mba-pipeline.mba_{0}.products_images` t1 on t0.asin = t1.asin
    left join `mba-pipeline.mba_{0}.products_details` t2 on t0.asin = t2.asin
    left join `mba-pipeline.mba_{0}.products_mba_images` t3 on t0.asin = t3.asin

    union all 
    
    SELECT t0.*, t2.title, t2.brand, DATE_DIFF(current_date(), Date(t2.upload_date), DAY) as time_since_upload,Date(t2.upload_date) as upload_date, t2.product_features, t1.url, t3.url_image_hq as url_mba_hq, t3.url_image_lowq as url_mba_lowq, t3.url_image_q2, t3.url_image_q3, t3.url_image_q4 FROM (
    SELECT asin, AVG(price) as price_mean,MAX(price) as price_max,MIN(price) as price_min,
            AVG(bsr) as bsr_mean, MAX(bsr) as bsr_max,MIN(bsr) as bsr_min,
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
    --where asin = 'B07PHZLLG1'
    order by t_fin.bsr_mean desc
    {1}
    """.format(marketplace, SQL_LIMIT)
    return SQL_STATEMENT


def get_dev_str(dev):
    dev_str = ""
    if dev:
        dev_str = "_dev"
    return dev_str


def create_plot_price_data(df_asin_detail_daily):
    """
        returns comma seperated plot data for price
        df_asin_detail_daily should also contain lines with bsr = 0. Shirts have bsr 0 if they do not already have a bsr.
        But in they still can have a price and should not be excluded
    """
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


def create_plot_data(df_asin_detail_daily):
    x_plot = ""
    y_plot = ""
    for bsr, date in zip(df_asin_detail_daily["bsr"].tolist(), df_asin_detail_daily["date"].tolist()):
        if bsr not in [0, 404]:
            x_plot = x_plot + "," + date.strftime("%d/%m/%Y")
            y_plot = y_plot + "," + str(bsr)
    return x_plot[1:], y_plot[1:]


def get_default_category_name(marketplace):
    if marketplace == "de":
        return "Fashion"
    else:
        return "Clothing, Shoes & Jewelry"

def get_bsr_top_category_names_list(marketplace):
    if marketplace == "de":
        return ["Fashion", "Bekleidung"]
    else:
        return ["Clothing, Shoes & Jewelry"]

def get_bsr_category(df_row, marketplace):
    if marketplace == "de":
        try:
            bsr_category = df_row["array_bsr_categorie"].strip("[]").split(",")[
                0].strip("'")
        except Exception as e:
            print(df_row["array_bsr_categorie"])
            print("Could not extract bsr_category", str(e))
            bsr_category = ""
    else:
        # does not split "," which does not work for "Clothing, Shoes & Jewelry"
        try:
            bsr_category = re.findall(
                "'([^']*)'", df_row["array_bsr_categorie"].strip("[]"))[0]
        except Exception as e:
            print(df_row["array_bsr_categorie"])
            print("Could not extract bsr_category", str(e))
            bsr_category = ""
    if bsr_category == "404" or bsr_category == "":
        bsr_category = get_default_category_name(marketplace)
    return bsr_category


def get_change_total(current, previous):
    current = float(current)
    previous = float(previous)
    if current == previous:
        return 0
    try:
        return current - previous
    except ZeroDivisionError:
        return 0

def get_first_ue_zero_by_col(df_asin_detail_daily, filter_col):
    # code is not nice but fast
    # return df_row which is nearest to present and bsr unequal to zero. If only zero values exists last row is returned
    # df_asin_detail_daily must be sorted by date where first value is near present and last value in past
    i = 0
    while True:
        try:
            first_ue_zero = df_asin_detail_daily.iloc[i]
        except:
            first_ue_zero = df_asin_detail_daily.iloc[0]
            break
        if int(first_ue_zero[filter_col]) != 0:
            break
        i += 1
    return first_ue_zero

def get_last_ue_zero_by_col(df_asin_detail_daily, filter_col):
    # code is not nice but fast    
    # return df_row which is nearest to present and bsr unequal to zero. If only zero values exists last row is returned
    # df_asin_detail_daily must be sorted by date where first value is near present and last value in past
    i = 1
    while True:
        try:
            last_ue_zero = df_asin_detail_daily.iloc[-i]
        except:
            last_ue_zero = df_asin_detail_daily.iloc[-1]
            break
        if int(last_ue_zero[filter_col]) != 0:
            break
        i += 1
    i = 1
    return last_ue_zero


def get_row_n_days_ago(df_asin_detail_daily, latest_occ_bsr_ue_zero, n_days=30):
    # try to first occurence 4 weeks in the past
    # if not possible use the first occurence of bsr un equal to zero
    date_N_weeks_ago = datetime.now() - timedelta(days=n_days)
    try:
        # make sure that occ_n_days contains an value unequal to zero if existent
        df_asin_detail_daily_n_days_ago = df_asin_detail_daily.loc[(
            df_asin_detail_daily.bsr > 0) & (df_asin_detail_daily.date < date_N_weeks_ago.date())]
        if len(df_asin_detail_daily_n_days_ago) == 0:
            # case we have no new bsr data crawled in last month. Therefore bsr_change should be prevented to contain multiple months between first and last bsr
            # results in bsr_change = 0
            occ_n_days = latest_occ_bsr_ue_zero
        else:
            occ_n_days = df_asin_detail_daily_n_days_ago.iloc[0]
    except Exception as e:
        print(str(e))
        occ_n_days = latest_occ_bsr_ue_zero
    return occ_n_days


def get_last_price(df_asin_detail_daily, latest_occ_price_ue_zero):
    # if amazon de is crawled with proxy of us amazon does not show prices, because products are not shipped into the us.
    # Therefore, many prices exists with 0. If no price could be crawled by daily crawler, the overview product page price should be used
    if latest_occ_price_ue_zero["price"] == 0:
        try:
            price_last = df_asin_detail_daily.iloc[0]["price_overview"]
        except:
            price_last = latest_occ_price_ue_zero["price"]
    else:
        price_last = latest_occ_price_ue_zero["price"]

    return price_last

def get_default_initial_additional_data_dict(marketplace):
    additional_data_dict = {}
    category_name = get_default_category_name(marketplace)
    for i, data_col in enumerate(ADDITIONAL_DATA_COLS):
        # default value are 0 for all values except last one is bsr_category
        if i != (len(ADDITIONAL_DATA_COLS) - 1):
            additional_data_dict[data_col] = 0
        else:
            additional_data_dict[data_col] = category_name
    return additional_data_dict
                    
def get_initial_additional_data_dict(marketplace, df_asin_detail_daily):
    if len(df_asin_detail_daily) == 0:
        return get_default_initial_additional_data_dict(marketplace)

    #time_start = time.time()
    # last row in df is more in past and therefore the first time shirt was crawled/occured in database
    latest_occ_bsr_ue_zero = get_first_ue_zero_by_col(df_asin_detail_daily, "bsr")
    oldest_occ_bsr_ue_zero = get_last_ue_zero_by_col(df_asin_detail_daily, "bsr")
    latest_occ_price_ue_zero = get_first_ue_zero_by_col(df_asin_detail_daily, "price")
    oldest_occ_price_ue_zero = get_last_ue_zero_by_col(df_asin_detail_daily, "price")

    oldest_occ = df_asin_detail_daily.iloc[-1]
    occ_n_days = get_row_n_days_ago(
        df_asin_detail_daily, latest_occ_bsr_ue_zero, n_days=30)
    #print("elapsed time", (time.time()-time_start))
    price_last = get_last_price(df_asin_detail_daily, latest_occ_price_ue_zero)
    bsr_category = get_bsr_category(latest_occ_bsr_ue_zero, marketplace)

    # additional_data_dict["bsr_last"].append(latest_occ_bsr_ue_zero["bsr"])
    # additional_data_dict["price_last"].append(price_last)
    # additional_data_dict["bsr_first"].append(oldest_occ["bsr"])
    # additional_data_dict["price_first"].append(oldest_occ_price_ue_zero["price"])
    # additional_data_dict["bsr_change"].append(get_change_total(latest_occ_bsr_ue_zero["bsr"], occ_n_days["bsr"]))
    # additional_data_dict["bsr_change_total"].append(get_change_total(latest_occ_bsr_ue_zero["bsr"], oldest_occ_bsr_ue_zero["bsr"]))
    # additional_data_dict["price_change"].append(get_change_total(latest_occ_price_ue_zero["price"], oldest_occ_price_ue_zero["price"]))
    # additional_data_dict["update_last"].append(latest_occ_bsr_ue_zero["date"])
    # additional_data_dict["score_last"].append(latest_occ_bsr_ue_zero["customer_review_score_mean"])
    # additional_data_dict["score_count"].append(latest_occ_bsr_ue_zero["customer_review_count"])
    # additional_data_dict["bsr_category"].append(bsr_category)
    additional_data_dict = {"bsr_last": latest_occ_bsr_ue_zero["bsr"], 
                            "price_last": price_last,
                            "bsr_first": oldest_occ["bsr"],
                            "price_first": oldest_occ_price_ue_zero["price"], 
                            "bsr_change": get_change_total(latest_occ_bsr_ue_zero["bsr"], occ_n_days["bsr"]),
                            "bsr_change_total": get_change_total(latest_occ_bsr_ue_zero["bsr"], oldest_occ_bsr_ue_zero["bsr"]), 
                            "price_change": get_change_total(latest_occ_price_ue_zero["price"], oldest_occ_price_ue_zero["price"]), 
                            "update_last": latest_occ_bsr_ue_zero["date"], 
                            "score_last": latest_occ_bsr_ue_zero["customer_review_score_mean"], 
                            "score_count": latest_occ_bsr_ue_zero["customer_review_count"], 
                            "bsr_count": len(df_asin_detail_daily),
                            "bsr_category": bsr_category}
    return additional_data_dict



def get_additional_data_dict(df_asin_detail_daily, marketplace="de", times = [0, 0, 0, 0], *args, **kwargs):
    # get plot data
    #print("Start to get plot data of shirts")
    start_time = time.time()
    plot_x, plot_y = create_plot_data(df_asin_detail_daily)
    plot_x_price, plot_y_price = create_plot_price_data(
        df_asin_detail_daily)
    times[1] = times[1] + (time.time() - start_time)
    #print("elapsed time: %.2f sec" %((time.time() - start_time)))

    #print("Start to get first and last bsr of shirts")
    # start_time = time.time()
    # bsr_last, price_last, bsr_first, price_first, bsr_change, bsr_change_total, price_change, update_last, score_last, score_count, bsr_category = get_first_and_last_data(
    #     df_asin_detail_daily, marketplace=marketplace)
    # additional_dict = {"bsr_last": bsr_last, "price_last": price_last, "bsr_first": bsr_first, "price_first": price_first, "bsr_change": bsr_change,
    #                    "bsr_change_total": bsr_change_total, "price_change": price_change, "update_last": update_last, "score_last": score_last, "score_count": score_count, "bsr_category": bsr_category}
    # times[2] = times[2] + (time.time() - start_time)
    
    start_time = time.time()
    additional_data_dict = get_initial_additional_data_dict(marketplace, df_asin_detail_daily)
    times[2] = times[2] + (time.time() - start_time)

    # additional_data_dict["plot_x"].append(plot_x)
    # additional_data_dict["plot_y"].append(plot_y)
    # additional_data_dict["plot_x_price"].append(plot_x_price)
    # additional_data_dict["plot_y_price"].append(plot_y_price)
    additional_data_dict.update({"plot_x": plot_x, "plot_y": plot_y,
                             "plot_x_price": plot_x_price, "plot_y_price": plot_y_price})

    # add takedown data
    start_time = time.time()
    is_takedown, takedown_date = get_takedown_data(df_asin_detail_daily)
    # additional_data_dict["takedown"].append(is_takedown)
    # additional_data_dict["takedown_date"].append(takedown_date)
    additional_data_dict.update(
        {"takedown": is_takedown, "takedown_date": takedown_date})

    additional_data_dict["asin"] = df_asin_detail_daily.iloc[0]["asin"]

    times[3] = times[3] + (time.time() - start_time)

    # start_time = time.time()
    # df_additional_data = df_additional_data.append(
    #     additional_dict, ignore_index=True)
    # times[0] = times[0] + (time.time() - start_time)

    # test if dict is correct
    additional_cols = ADDITIONAL_DATA_COLS + ["plot_x", "plot_y", "plot_x_price", "plot_y_price", "takedown", "takedown_date", "asin"]
    assert is_additional_data_dict_valid(additional_data_dict, additional_cols), "One column of additional_data_dict has different length to other one"
    
    return additional_data_dict

def is_additional_data_dict_valid(additional_data_dict, additional_cols):
    #init_length = len(additional_data_dict[additional_cols[0]])
    for additional_col in additional_cols:
        if additional_col not in additional_data_dict:
            return False
    return True

def get_df_additional_data(marketplace, df_shirts_detail_daily, use_dask=False, num_threads=20):
    start_time_total = time.time()
    times = [0, 0, 0, 0]
    additional_data_dicts = []
    #print("THREADS", num_threads)
    pool = ThreadPool(num_threads) 

    #for data_col in additional_cols:
    #    additional_data_dict[data_col] = []
    
    #dask specific code
    if use_dask:
        test = df_shirts_detail_daily.groupby('asin').apply(get_additional_data_dict, meta={marketplace: "str"}).compute()
    else:
        start_time = time.time()
        grouped_by_asin = df_shirts_detail_daily.groupby(["asin"])
        count_groups = len(grouped_by_asin)
        df_asin_detail_daily_list = []
        thread_counter = 0
        group_counter = 0
        for asin, df_asin_detail_daily in grouped_by_asin:
            group_counter += 1
            if start_time:
                times[0] = times[0] + (time.time() - start_time)
            # additional_data_dict = get_additional_data_dict(df_asin_detail_daily, marketplace=marketplace, times=times)
            # additional_data_dicts.append(additional_data_dict)

            df_asin_detail_daily_list.append(df_asin_detail_daily)
            thread_counter += 1
            # execute multithreading if num_threads is reached or if iteration over group is in last iteration
            if thread_counter == num_threads or group_counter == count_groups:
                get_additional_data_dict_fn = partial(get_additional_data_dict, marketplace=marketplace, times=times)
                # additional_data_dict = get_additional_data_dict(df_asin_detail_daily, marketplace=marketplace, times=times)
                additional_data_dict_list_by_threads = pool.map(get_additional_data_dict_fn, df_asin_detail_daily_list)
                start_time = time.time()
                additional_data_dicts.extend(additional_data_dict_list_by_threads)
                
                # set back to default
                thread_counter = 0
                df_asin_detail_daily_list = []

    df_additional_data = pd.DataFrame(additional_data_dicts)
    print("Elapsed time for 1. df prepare, 2. plot data, 3. last/first, 4. takedown",
        times, "total time: %.2f sec" % (time.time() - start_time_total))
    return df_additional_data


def get_takedown_data(df_asin_detail_daily):
    """
        Return: is_takedown (bool), takedown_date (None if is_takedown is False)
    """
    price_dates = df_asin_detail_daily["date"].tolist()
    price_data = df_asin_detail_daily["price"].tolist()
    for i, price in enumerate(price_data):
        if price == 404:
            return True, price_dates[i]
    return False, None

# def get_takedown_lists(df_asins, df_shirts_detail_daily):
#     takedown_data = df_asins.apply(lambda x: get_takedown_data(x, df_shirts_detail_daily), axis=1)
#     takedown_list = []
#     takedown_date_list = []
#     for takedown_data_i in takedown_data:
#         takedown_list.append(takedown_data_i[0])
#         takedown_date_list.append(takedown_data_i[1])
#     return takedown_list, takedown_date_list


def append_df_shirts_with_more_info(marketplace, df_shirts, df_shirts_with_more_info, df_shirts_asin_chunk, chunk_size_csv_file, num_threads):
    use_dask = False
    asin_list = df_shirts_asin_chunk["asin"].tolist()
    print("Start to get chunk from bigquery")
    start_time = time.time()
    try:
        # new cost effective method with local file
        df_shirts_detail_daily = BigqueryHandler.get_product_details_daily_data_by_asin(
            marketplace, asin_list, chunksize=chunk_size_csv_file, use_dask=use_dask).drop_duplicates()
    except Exception as e:
        print(str(e))
        raise ValueError
    df_shirts_detail_daily["date"] = df_shirts_detail_daily.apply(
        lambda x: x["timestamp"].date(), axis=1)
    # drop bsr data with same date (multiple times crawled on same day)
    df_shirts_detail_daily = df_shirts_detail_daily.drop_duplicates([
                                                                    "asin", "date"])
    print("Got bigquery chunk. elapsed time: %.2f sec" %
          ((time.time() - start_time)))

    start_time_additional_data = time.time()
    df_additional_data = get_df_additional_data(
        marketplace, df_shirts_detail_daily, use_dask=use_dask, num_threads=num_threads)
    print("elapsed time for additional_data: %.2f sec" %
          ((time.time() - start_time_additional_data)))

    df_shirts_with_more_info_append = df_shirts.merge(df_additional_data, left_on='asin', right_on='asin')
    #                                                  left_index=True, right_index=True)
    df_shirts_with_more_info = df_shirts_with_more_info.append(
        df_shirts_with_more_info_append, ignore_index=True)
    return df_shirts_with_more_info


def change_outlier_with_max(list_with_outliers, q=90):
    value = np.percentile(list_with_outliers, q)
    print(value)
    for i in range(len(list_with_outliers)):
        if list_with_outliers[i] > value:
            list_with_outliers[i] = value
    # return list_with_outliers


def add_value_to_older_shirts(x_scaled, index_privileged, add_value, add_value_newer=0.05):
    for i in range(len(x_scaled)):
        if i > index_privileged:
            x_scaled[i] = x_scaled[i] + add_value
        else:
            x_scaled[i] = x_scaled[i] + add_value_newer


def power(my_list):
    '''Exponential growth
    '''
    return [x**3 for x in my_list]


def make_trend_column(marketplace, df_shirts, months_privileged=6):
    df_shirts = df_shirts.sort_values(
        "time_since_upload").reset_index(drop=True)
    # get list of integers with time since upload days
    x = df_shirts[["time_since_upload"]].values
    # fill na with max value
    x = np.nan_to_num(x, np.nanmax(x))
    # get index of last value within privileged timezone
    index_privileged = len([v for v in x if v < 30*months_privileged]) - 1
    # transform outliers to max value before outliers
    x_without_outliers = x.copy()
    change_outlier_with_max(x_without_outliers)
    min_max_scaler = preprocessing.MinMaxScaler()
    # scale list to values between 0 and 1
    x_scaled = min_max_scaler.fit_transform(x_without_outliers)
    # add add_value to scaled list to have values < 1 reduce trend and values < 1 increase it
    add_value = 1 - x_scaled[index_privileged]
    add_value_to_older_shirts(x_scaled, index_privileged, add_value)
    #x_scaled = x_scaled + add_value
    # power operation for exponential change 0 < x < (1+add_value)**3
    x_power = power(x_scaled)
    df = pd.DataFrame(x_power)
    df_shirts["time_since_upload_power"] = df.iloc[:, 0]
    # change bsr_last to high number to prevent distort trend calculation
    df_shirts.loc[~(df_shirts['bsr_category'].isin(get_bsr_top_category_names_list(
        marketplace))), "bsr_last"] = 999999999
    df_shirts.loc[(df_shirts['bsr_last'] == 0.0), "bsr_last"] = 999999999
    df_shirts.loc[(df_shirts['bsr_last'] == 404.0), "bsr_last"] = 999999999
    df_shirts["trend"] = df_shirts["bsr_last"] * \
        df_shirts["time_since_upload_power"]
    df_shirts = df_shirts.sort_values(
        "trend", ignore_index=True).reset_index(drop=True)
    df_shirts["trend_nr"] = df_shirts.index + 1
    return df_shirts


def create_change_columns(marketplace, df_shirts_with_more_info, dev_str="", project_id="mba-pipeline"):
    """ Add columns of change between yesterday/last merchwatch_shirts table and today/new one
    """
    # try to calculate trend change
    df_shirts_old = pd.read_gbq("SELECT DISTINCT asin, trend_nr, bsr_last, bsr_change FROM mba_" +
                                str(marketplace) + ".merchwatch_shirts" + dev_str, project_id=project_id)
    df_shirts_old["trend_nr_old"] = df_shirts_old["trend_nr"].astype(int)
    df_shirts_old["bsr_last_old"] = df_shirts_old["bsr_last"].astype(int)
    df_shirts_old["bsr_change_old"] = df_shirts_old["bsr_change"].astype(int)
    # transform older trend nr (yesterday) in same dimension as new trend nr
    df_shirts_with_more_info = df_shirts_with_more_info.merge(
        df_shirts_old[["asin", "trend_nr_old", "bsr_last_old", "bsr_change_old"]], how='left', on='asin')
    try:
        df_shirts_with_more_info['trend_nr_old'] = df_shirts_with_more_info['trend_nr_old'].fillna(
            value=0).astype(int)
        df_shirts_with_more_info["trend_change"] = df_shirts_with_more_info.apply(
            lambda x: 0 if int(x["trend_nr_old"]) == 0 else int(x["trend_nr_old"] - x["trend_nr"]), axis=1)
    except Exception as e:
        df_shirts_with_more_info["trend_change"] = 0
    # try to create should_be_updated column
    df_shirts_with_more_info['bsr_last_old'] = df_shirts_with_more_info['bsr_last_old'].fillna(
        value=0).astype(int)
    df_shirts_with_more_info['bsr_last_change'] = (
        df_shirts_with_more_info["bsr_last_old"] - df_shirts_with_more_info["bsr_last"]).astype(int)

    return df_shirts_with_more_info


def create_should_be_updated_column(df_shirts_with_more_info):
    """ Created column 'should_be_updated' indicated whether or not firestore document should be updated
    """
    # TODO: Find out why designs which got taken down do not get flag should_be_updated
    # get the date one week ago
    date_one_week_ago = (datetime.now() - timedelta(days=7)).date()
    if len(df_shirts_with_more_info) <= 1000:
        bsr_change_threshold = df_shirts_with_more_info.sort_values(
            by=['bsr_change']).iloc[len(df_shirts_with_more_info)-1]["bsr_change"]
    else:
        bsr_change_threshold = df_shirts_with_more_info.sort_values(
            by=['bsr_change']).iloc[1000]["bsr_change"]
    # filter df which should always be updated (update newer than 7 days + bsr_count equals 1 or 2 or trend_nr lower or equal to 2000 or bsr_change is within top 1000)
    df_should_update = df_shirts_with_more_info[((df_shirts_with_more_info["bsr_count"] <= 2) & (df_shirts_with_more_info["update_last"] >= date_one_week_ago)) | (
        df_shirts_with_more_info["trend_nr"] <= 2000) | (df_shirts_with_more_info["bsr_change"] < bsr_change_threshold) | (df_shirts_with_more_info["bsr_change_old"] < bsr_change_threshold)]
    # change bsr_last_change to 1 for those how should be updated independent of bsr_last
    df_shirts_with_more_info.loc[df_should_update.index, "bsr_last_change"] = 1
    df_shirts_with_more_info['should_be_updated'] = df_shirts_with_more_info['bsr_last_change'] != 0

    return df_shirts_with_more_info


def replace_price_last_zero(marketplace, project_id="mba-pipeline", dev_str=""):
    bq_client = bigquery.Client(project=project_id)

    SQL_STATEMENT = '''CREATE OR REPLACE TABLE `mba-pipeline.mba_{0}.merchwatch_shirts{1}`
    AS        
    SELECT CASE WHEN t0.price_last = 0.0 THEN  CAST(REPLACE(
        t1.price,
        ',',
        '.') as FLOAT64) ELSE t0.price_last END as price_last
    , t0.* EXCEPT(price_last) FROM `mba-pipeline.mba_{0}.merchwatch_shirts{1}` t0 
    LEFT JOIN (SELECT distinct asin, price FROM `mba-pipeline.mba_{0}.products`) t1 on t1.asin = t0.asin
    '''.format(marketplace, dev_str)

    query_job = bq_client.query(SQL_STATEMENT)
    query_job.result()


def update_bq_shirt_tables(marketplace, chunk_size=500, limit=None, filter=None, dev=False, project_id="mba-pipeline", num_threads=4):
    start_time = time.time()
    # This part should only triggered once a day to update all relevant data
    print("Load shirt data from bigquery")
    df_shirts = pd.read_gbq(get_sql_shirts(
        marketplace, limit=limit), project_id=project_id).drop_duplicates(["asin"])

    # This dataframe is expanded with additional information with every chunk
    df_shirts_with_more_info = pd.DataFrame()

    print("Chunk size: " + str(chunk_size))
    df_shirts_asin = df_shirts[["asin"]].copy()

    # if development than bigquery operations should only change dev tables
    dev_str = get_dev_str(dev)

    df_shirts_asin_chunks = [df_shirts_asin[i:i+chunk_size]
                             for i in range(0, df_shirts_asin.shape[0], chunk_size)]
    start_time_more_info = time.time()
    for i, df_shirts_asin_chunk in enumerate(df_shirts_asin_chunks):
        print("Chunk %s of %s" % (i + 1, len(df_shirts_asin_chunks)))
        start_time_chunk = time.time()
        df_shirts_with_more_info = append_df_shirts_with_more_info(
            marketplace, df_shirts, df_shirts_with_more_info, df_shirts_asin_chunk, chunk_size_csv_file=10000, num_threads=num_threads)
        gc.collect()
        print("elapsed time: %.2f sec" % ((time.time() - start_time_chunk)))
    print("elapsed time for more info task: %.2f sec" % ((time.time() - start_time_more_info)))

    df_shirts_with_more_info = make_trend_column(
        marketplace, df_shirts_with_more_info)
    df_shirts_with_more_info = create_change_columns(
        marketplace, df_shirts_with_more_info, dev_str="", project_id=project_id)
    df_shirts_with_more_info = create_should_be_updated_column(
        df_shirts_with_more_info)

    # save dataframe with shirts in local storage
    print("Length of dataframe", len(df_shirts_with_more_info), dev_str)
    table_path = "mba_" + str(marketplace) + ".merchwatch_shirts" + dev_str
    df_shirts_with_more_info.to_gbq(table_path, chunksize=10000, project_id="mba-pipeline", if_exists="replace")
    replace_price_last_zero(marketplace, project_id=project_id, dev_str=dev_str)
    print("Update merchwatch daily table completed. Elapsed time: %.2f minutes" % (
        (time.time() - start_time) / 60))


def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        'marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument(
        '--chunk_size', help='Chunk size of asins which should be used for on loop iteration', type=int, default=10000)
    parser.add_argument(
        '--debug_limit', help='Whether only limit of asins should be used for execution', type=int, default=None)
    parser.add_argument(
        '--num_threads', help='How many threads should be used for multiprocessing', type=int, default=4)
    parser.add_argument(
        '--dev', help='Whether its a dev execution or not', action='store_true')

    if "merchwatch_daily_creator" in argv[0]:
       argv = argv[1:]
    
    # get all arguments
    args = parser.parse_args(argv)
    marketplace = args.marketplace
    chunk_size = args.chunk_size
    debug_limit = args.debug_limit
    num_threads = args.num_threads
    dev = args.dev
    print(argv, dev)

    update_bq_shirt_tables(
        marketplace, chunk_size=chunk_size, limit=debug_limit, dev=dev, num_threads=num_threads)


if __name__ == '__main__':
    main(sys.argv)



'''
### Deprecated code
'''

# def get_plot_lists(df_asin_daily_data):
#     start_time = time.time()
#     plot_data = df_asins["asin"].apply(lambda asin: create_plot_data(asin, df_shirts_detail_daily))
#     print("elapsed time for bsr plot data: %.2f sec" %((time.time() - start_time)))
#     plot_x=[]
#     plot_y=[]
#     for plot_data_i in plot_data:
#         plot_x.append(plot_data_i[0])
#         plot_y.append(plot_data_i[1])
#     start_time = time.time()
#     plot_price_data = df_asins["asin"].apply(lambda asin: create_plot_price_data(asin, df_shirts_detail_daily))
#     print("elapsed time for price plot data: %.2f sec" %((time.time() - start_time)))
#     plot_x_price = []
#     plot_y_price = []
#     for plot_price_data_i in plot_price_data:
#         plot_x_price.append(plot_price_data_i[0])
#         plot_y_price.append(plot_price_data_i[1])
#     return plot_x, plot_y, plot_x_price, plot_y_price

# def get_ue_zero_rows(df_asin_detail_daily, n_days=30):
#     # performance optimated function
#     # init default values
#     last_row = df_asin_detail_daily.iloc[-1]
#     first_row = df_asin_detail_daily.iloc[0]
    
#     latest_occ_bsr_ue_zero = pd.Series() # first_row
#     oldest_occ_bsr_ue_zero = pd.Series() # last_row
#     latest_occ_price_ue_zero = pd.Series() # first_row
#     oldest_occ_price_ue_zero = pd.Series() # last_row
#     oldest_occ = last_row
#     occ_n_days = pd.Series() # first_row

#     date_N_weeks_ago = datetime.now() - timedelta(days=n_days)

#     i = 0
#     while True:
#         try:
#             df_asin_detail_daily_row = df_asin_detail_daily.iloc[i]
#         except:
#             break
#         if int(df_asin_detail_daily_row["bsr"]) != 0:
#             # do overwrite latest_occ_bsr_ue_zero only once at the beginning
#             if latest_occ_bsr_ue_zero.empty:
#                 latest_occ_bsr_ue_zero = df_asin_detail_daily_row

#         if int(df_asin_detail_daily_row["price"]) != 0:
#             # do overwrite latest_occ_price_ue_zero only once at the beginning
#             if latest_occ_price_ue_zero.empty:
#                 latest_occ_price_ue_zero = df_asin_detail_daily_row

#         if occ_n_days.empty and df_asin_detail_daily_row["date"] < date_N_weeks_ago.date():
#             occ_n_days = df_asin_detail_daily_row

#         if not latest_occ_bsr_ue_zero.empty and not latest_occ_price_ue_zero.empty and not occ_n_days.empty:
#             break

#         i += 1

#     i = 1
#     while True:
#         try:
#             df_asin_detail_daily_row = df_asin_detail_daily.iloc[-i]
#         except:
#             break
#         if int(df_asin_detail_daily_row["bsr"]) != 0:
#             # do overwrite oldest_occ_bsr_ue_zero only once at the beginning
#             if oldest_occ_bsr_ue_zero.empty:
#                 oldest_occ_bsr_ue_zero = df_asin_detail_daily_row

#         if int(df_asin_detail_daily_row["price"]) != 0:
#             # do overwrite oldest_occ_price_ue_zero only once at the beginning
#             if oldest_occ_price_ue_zero.empty:
#                 oldest_occ_price_ue_zero = df_asin_detail_daily_row
#         if not oldest_occ_price_ue_zero.empty and not oldest_occ_bsr_ue_zero.empty:
#             break
#         i += 1

#     # set defaults if matching df_row could not be found
#     if latest_occ_bsr_ue_zero.empty:
#         latest_occ_bsr_ue_zero = first_row
#     if latest_occ_price_ue_zero.empty:
#         latest_occ_price_ue_zero = first_row
#     if occ_n_days.empty:
#         occ_n_days = first_row
            
#     if oldest_occ_bsr_ue_zero.empty:
#         oldest_occ_bsr_ue_zero = last_row
#     if oldest_occ_price_ue_zero.empty:
#         oldest_occ_price_ue_zero = last_row

#     return latest_occ_bsr_ue_zero, oldest_occ_bsr_ue_zero, latest_occ_price_ue_zero, oldest_occ_price_ue_zero, oldest_occ, occ_n_days




# def get_first_and_last_row_bsr_ue_zero(df_asin_detail_daily):
#     df_filtered = df_asin_detail_daily.loc[df_asin_detail_daily.bsr > 0]
#     if not df_filtered.empty:
#         return df_filtered.iloc[0], df_filtered.iloc[-1]
#     else:
#         return df_asin_detail_daily.iloc[0], df_asin_detail_daily.iloc[-1]

# def get_first_and_last_row_price_ue_zero(df_asin_detail_daily):
#     # return df_row which is nearest to present and bsr unequal to zero. If only zero values exists last row is returned
#     # df_asin_detail_daily must be sorted by date where first value is near present and last value in past
#     df_filtered = df_asin_detail_daily.loc[df_asin_detail_daily.price > 0]
#     if not df_filtered.empty:
#         return df_filtered.iloc[0], df_filtered.iloc[-1]
#     else:
#         return df_asin_detail_daily.iloc[0], df_asin_detail_daily.iloc[-1]

# def get_first_and_last_data(df_asin_detail_daily, marketplace="de"):
#     # return last_bsr, last_price, first_bsr, first_price
#     if len(df_asin_detail_daily) == 0:
#         category_name = get_default_category_name(marketplace)
#         return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, category_name
#     else:
#         i = 0
#         # try to get last bsr which is unequal to zero. If only zero bsr exists return last occurence
#         while True:
#             try:
#                 last_occ = df_asin_detail_daily.iloc[i]
#             except:
#                 last_occ = df_asin_detail_daily.iloc[0]
#                 break
#             if int(last_occ["bsr"]) != 0:
#                 break
#             i += 1
#         i = 0
#         # try to get last price which is unequal to zero. If only zero bsr exists return last occurence
#         while True:
#             try:
#                 last_occ_price = df_asin_detail_daily.iloc[i]
#             except:
#                 last_occ_price = df_asin_detail_daily.iloc[0]
#                 break
#             if int(last_occ_price["price"]) != 0.0:
#                 break
#             i += 1
#         i = 1
#         # try to get first bsr which is unequal to zero. If only zero bsr exists return first occurence
#         while True:
#             try:
#                 oldest_occ_ue_zero = df_asin_detail_daily.iloc[-i]
#             except:
#                 oldest_occ_ue_zero = df_asin_detail_daily.iloc[-1]
#                 break
#             if int(oldest_occ_ue_zero["bsr"]) != 0:
#                 break
#             i += 1
#         i = 1
#         # try to get first price which is unequal to zero. If only zero bsr exists return first occurence
#         while True:
#             try:
#                 oldest_occ_price_ue_zero = df_asin_detail_daily.iloc[-i]
#             except:
#                 oldest_occ_price_ue_zero = df_asin_detail_daily.iloc[-1]
#                 break
#             if int(oldest_occ_price_ue_zero["price"]) != 0.0:
#                 break
#             i += 1
#     # get first occurence of data
#     oldest_occ = df_asin_detail_daily.iloc[-1]

#     # try to first occurence 4 weeks in the past
#     # if not possible use the first occurence of bsr un equal to zero
#     last_n_weeks = 4
#     days = 30
#     date_N_weeks_ago = datetime.now() - timedelta(days=days)
#     try:
#         # make sure that occ_4w contains an value unequal to zero if existent
#         df_asin_detail_daily_4w = df_asin_detail_daily[(
#             df_asin_detail_daily['date'] < date_N_weeks_ago.date()) & (df_asin_detail_daily['bsr'] != 0)]
#         if len(df_asin_detail_daily_4w) == 0:
#             # case we have no new bsr data crawled in last month. Therefore bsr_change should be prevented to contain multiple months between first and last bsr
#             # results in bsr_change = 0
#             occ_4w = last_occ
#         else:
#             occ_4w = df_asin_detail_daily_4w.iloc[0]
#     except Exception as e:
#         print(str(e))
#         occ_4w = last_occ

#     if last_occ_price["price"] == 0:
#         try:
#             price_last = df_asin_detail_daily.iloc[0]["price_overview"]
#         except:
#             price_last = last_occ_price["price"]
#     else:
#         price_last = last_occ_price["price"]
#     bsr_category = get_bsr_category(last_occ, marketplace)

#     return last_occ["bsr"], price_last, oldest_occ["bsr"], oldest_occ_price_ue_zero["price"], get_change_total(last_occ["bsr"], occ_4w["bsr"]), get_change_total(last_occ["bsr"], oldest_occ["bsr"]), get_change_total(last_occ["price"], oldest_occ["price"]), last_occ["date"], last_occ["customer_review_score_mean"], last_occ["customer_review_count"], bsr_category
