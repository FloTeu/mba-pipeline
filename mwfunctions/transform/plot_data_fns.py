from rdp import rdp
import copy
import time
import random
import collections
from datetime import date, datetime

from mwfunctions.pydantic.bigquery_classes import BQPlotDataRaw

max_number_of_plot_points = 20



def get_short_list(list, mask, convert_to_str=False):
    if convert_to_str:
        return [str(value) for i, value in enumerate(list) if mask[i]]
    else:
        return [value for i, value in enumerate(list) if mask[i]]


def get_epsilon(highest_epsilon, max_plot_data_count, bsr_list_length, iteration_count):
    return highest_epsilon / max_plot_data_count * bsr_list_length + (iteration_count * 20)


def shorten_by_rdp(value_list_short, key_list_short, epsilon):
    rdp_mask = rdp([[i, int(v)] for i, v in enumerate(value_list_short)], epsilon=epsilon, return_mask=True)
    return get_short_list(value_list_short, rdp_mask, convert_to_str=False), get_short_list(key_list_short, rdp_mask,
                                                                                            convert_to_str=False)


def remove_random(value_list_short_i, key_list_short_i, max_number_of_plot_points, min_number_of_plot_points):
    assert len(value_list_short_i) == len(
        key_list_short_i), "Lists value_list_short_i, key_list_short_i need to have same length"
    value_list_short = copy.deepcopy(value_list_short_i)
    key_list_short = copy.deepcopy(key_list_short_i)
    # if loop can not get right range, force it by randomly remove points between start and end
    while len(value_list_short) < min_number_of_plot_points or len(value_list_short) > max_number_of_plot_points:
        index_to_pop = random.randint(1, len(value_list_short) - 1)
        value_list_short.pop(index_to_pop)
        key_list_short.pop(index_to_pop)
    return value_list_short, key_list_short


def get_shortened_plot_data_dict(plot_data_dict, max_number_of_plot_points=20, min_number_of_plot_points=18):
    time_start = time.time()
    shortened_plot_data_dict = {}

    plot_data_dict_od = collections.OrderedDict(sorted(plot_data_dict.items()))
    key_list = list(plot_data_dict_od.keys())
    value_list = list(plot_data_dict_od.values())
    value_list_length = len(value_list)
    value_list_short = []
    key_list_short = []
    if value_list_length > max_number_of_plot_points:
        # init so that while loop not directly breaks
        value_list_short = copy.deepcopy(value_list)
        key_list_short = copy.deepcopy(key_list)
        iteration_count = 0
        # while bsr_list_short has more points than max_number_points, we want to decrease it
        while len(value_list_short) > max_number_of_plot_points:
            try:
                # init with 0.5 because 0 would have no effect in most cases
                epsilon = 0.5 + iteration_count  # get_epsilon(highest_epsilon, plot_data_count, value_list_length, iteration_count)
                # print("Epsilon", epsilon, iteration_count)
                value_list_short_i, key_list_short_i = shorten_by_rdp(value_list_short, key_list_short, epsilon)

                # length is to low. Therefore we use rdp with other params / lower epsilon
                if len(value_list_short_i) < min_number_of_plot_points:
                    epsilon_decrease = 0.1
                    iteration_count_internal = 1
                    while len(value_list_short_i) < min_number_of_plot_points:
                        # decrease epsilon in first times only little bit and with increasing iteration_count_internal epsilon decrease becomes higher
                        epsilon = epsilon - (epsilon_decrease * iteration_count_internal)
                        if epsilon < 0:
                            # value_list_short_i has less elements than min_number_of_plot_points. Threfore remove_random should take larger lists value_list_short, key_list_short
                            value_list_short_i, key_list_short_i = remove_random(value_list_short, key_list_short,
                                                                                 max_number_of_plot_points,
                                                                                 min_number_of_plot_points)
                            break
                        # print(epsilon)
                        value_list_short_i, key_list_short_i = shorten_by_rdp(value_list_short, key_list_short, epsilon)

                        iteration_count_internal = iteration_count_internal + 1
                        # make sure while loop does not take endless time
                        if len(value_list_short_i) >= max_number_of_plot_points:
                            value_list_short_i, key_list_short_i = remove_random(value_list_short_i, key_list_short_i,
                                                                                 max_number_of_plot_points,
                                                                                 min_number_of_plot_points)
                            break
                # overwrite short lists
                value_list_short = value_list_short_i
                key_list_short = key_list_short_i
                iteration_count = iteration_count + 1

            except Exception as e:
                print("ERROR while try to short data for plot", str(e))
                break
    else:
        value_list_short = value_list
        key_list_short = key_list

    for i, key in enumerate(key_list_short):
        shortened_plot_data_dict[key] = value_list_short[i]

    # print("elapsed time %.2f" % (time.time() - time_start))
    return shortened_plot_data_dict


def get_shortened_plot_data(sub_collection_dict, max_number_of_plot_points=20, min_number_of_plot_points=18):
    """
        sub_collection_dict:
            {
                "plot_data":
                    {
                        "year":
                            {"bsr": {"2020-09-20": 480549, ...},
                            "price": {"2020-09-20": 13.99, ...}
                            }
                    }
            }

        returns:
            shortened_plot_data:
                {
                    "bsr_short":
                        {"2020-09-20": 480549, ...}
                    "prices_short":
                        {"2020-09-20": 480549, ...}
                }

    """
    shortened_plot_data = {}
    if "plot_data" in sub_collection_dict:
        for plot_key in ["bsr", "prices", "scores"]:
            plot_dict = {}
            for year in sub_collection_dict["plot_data"].keys():
                if plot_key in sub_collection_dict["plot_data"][year]:
                    plot_dict.update(sub_collection_dict["plot_data"][year][plot_key])
            shortened_plot_data_dict = get_shortened_plot_data_dict(plot_dict,
                                                                    max_number_of_plot_points=max_number_of_plot_points,
                                                                    min_number_of_plot_points=min_number_of_plot_points)
            # if plot_key == "bsr":
            #    print(len(shortened_plot_data_dict.keys()))
            shortened_plot_data[plot_key + "_short"] = shortened_plot_data_dict
        return shortened_plot_data
    else:
        return shortened_plot_data


## BQ to FS

def list2year_dict(data_list, date_list, year_dict, data_name, date_format='%d/%m/%Y') -> dict:
    """
        data_list: [2312, 23423423, 43534]
        date_list: [03/07/2021,01/07/2021,17/06/2021]
        data_name: e.g. bsr, price etc.

        year_dict = {2022: {"bsr": ...}}
    """
    assert len(data_list) == len(
        date_list), f"data_list and date_list need to have same length, but have length {len(data_list)} and {len(date_list)}"

    while len(date_list) != 0:
        date_str = date_list.pop(0)
        data = data_list.pop(0)

        year = str(datetime.strptime(date_str, date_format).year)
        date_str_standard = str(datetime.strptime(date_str, date_format).date())
        if year not in year_dict:
            year_dict[year] = {}
        if data_name not in year_dict[year]:
            year_dict[year][data_name] = {}
        year_dict[year][data_name].update({date_str_standard: data})
    return year_dict


def df_dict2subcollections(df_dict: BQPlotDataRaw, date_format='%d/%m/%Y'):
    """
    """
    from mwfunctions.pydantic.firestore.mba_shirt_classes import FSWatchItemSubCollectionDict
    sub_collection_dict = {}

    dates_bsr_list = []
    bsr_data_list = []
    dates_price_list = []
    price_data_list = []

    if "plot_x" in df_dict and df_dict["plot_x"] != None:
        dates_bsr_list = df_dict["plot_x"].split(",")
    if "plot_y" in df_dict and df_dict["plot_y"] != None:
        bsr_data_list = [int(v) for v in df_dict["plot_y"].split(",")]
    if "plot_x_price" in df_dict and df_dict["plot_x_price"] != None:
        dates_price_list = df_dict["plot_x_price"].split(",")
    if "plot_y_price" in df_dict and df_dict["plot_y_price"] != None:
        price_data_list = [float(v) for v in df_dict["plot_y_price"].split(",")]

    if len(dates_bsr_list) == 0:
        curr_year = datetime.now().year
        return {"plot_data": {str(curr_year): {"bsr": {}, "prices": {}, "takedowns": {}, "uploads": {}, "year": curr_year}}}

    plot_data_dict = {}

    if len(dates_price_list) > 0:
        start_year = min(datetime.strptime(dates_bsr_list[-1], '%d/%m/%Y').year,
                         datetime.strptime(dates_price_list[-1], '%d/%m/%Y').year)
        end_year = max(datetime.strptime(dates_bsr_list[0], '%d/%m/%Y').year,
                       datetime.strptime(dates_price_list[0], '%d/%m/%Y').year)
    else:
        start_year = datetime.strptime(dates_bsr_list[-1], '%d/%m/%Y').year
        end_year = datetime.strptime(dates_bsr_list[0], '%d/%m/%Y').year

    plot_data_dict = list2year_dict(bsr_data_list, dates_bsr_list, plot_data_dict, "bsr", date_format=date_format)
    plot_data_dict = list2year_dict(price_data_list, dates_price_list, plot_data_dict, "prices", date_format=date_format)

    # standardize plot_data dict. Every year should contain year as field + bsr and prices as at least empty dicts
    for year_count in range(end_year - start_year + 1):
        curr_year = start_year + year_count
        curr_year_str = str(curr_year)
        if curr_year_str not in plot_data_dict:
            plot_data_dict[curr_year_str] = {}
        if "year" not in plot_data_dict[curr_year_str]:
            plot_data_dict[curr_year_str]["year"] = curr_year
        for data_name in ["bsr", "prices"]:
            if data_name not in plot_data_dict[curr_year_str]:
                plot_data_dict[curr_year_str][data_name] = {}

    sub_collection_dict.update({"plot_data": plot_data_dict})

    return FSWatchItemSubCollectionDict.parse_obj(sub_collection_dict)