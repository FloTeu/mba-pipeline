import numpy as np

from sklearn import preprocessing
from datetime import date, datetime, timedelta
from typing import List, Optional

# list of multiplicator values to bsr value to get trend score. Index is defined by days since upload of product
TREND_SCORE_MULTIPLICATOR_LIST_CACHE: Optional[List[float]] = None

def get_trend_score_multiplicator_list(month_decreasing_trend=6):
    days_until_first_upload_on_mba = (
                datetime.now() - datetime(2015, 1, 1)).days  # mba started around 2015 for us marketplace
    days_until_month_decreasing_trend = 30 * month_decreasing_trend
    upload_since_days_list = list(range(days_until_first_upload_on_mba))

    min_max_scaler = preprocessing.MinMaxScaler()
    # scale list to values between 0 and 1
    x_scaled = min_max_scaler.fit_transform(np.array(upload_since_days_list).reshape(-1, 1))
    # add add_value to scaled list to have values < 1 reduce trend and values < 1 increase it
    add_value = 1 - x_scaled[days_until_month_decreasing_trend]
    for i in range(len(x_scaled)):
        if i > days_until_month_decreasing_trend:
            x_scaled[i] = x_scaled[i] + add_value
        else:
            x_scaled[i] = x_scaled[i] + 0.05  # prevent to low values for first entries
    return np.squeeze([x ** 3 for x in x_scaled]).tolist()

def update_trend_score_multiplicator_list():
    # TODO: What happens if instance is always on. cache should be cleaned once a day
    global TREND_SCORE_MULTIPLICATOR_LIST_CACHE
    TREND_SCORE_MULTIPLICATOR_LIST_CACHE = get_trend_score_multiplicator_list()

def get_trend_multiplicator(days_until_upload):
    # returns a multiplcator value which can be applied to bsr to get trend score
    global TREND_SCORE_MULTIPLICATOR_LIST_CACHE
    if not TREND_SCORE_MULTIPLICATOR_LIST_CACHE:
        TREND_SCORE_MULTIPLICATOR_LIST_CACHE = get_trend_score_multiplicator_list()
    try:
        return TREND_SCORE_MULTIPLICATOR_LIST_CACHE[days_until_upload]
    except IndexError:
        return TREND_SCORE_MULTIPLICATOR_LIST_CACHE[-1] # for upload earlier than defined in TREND_SCORE_MULTIPLICATOR_LIST, last element should be returned