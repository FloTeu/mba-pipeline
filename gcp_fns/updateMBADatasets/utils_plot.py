from rdp import rdp
import copy
import time
import random

max_number_of_plot_points = 20

def get_short_list(list, mask, convert_to_str=False):
    if convert_to_str:
        return [str(value) for i, value in enumerate(list) if mask[i]]
    else:
        return [value for i, value in enumerate(list) if mask[i]]

def get_epsilon(highest_epsilon, max_plot_data_count, bsr_list_length, iteration_count):
    return highest_epsilon/max_plot_data_count*bsr_list_length + (iteration_count * 20)

def shorten_by_rdp(value_list_short, key_list_short, epsilon):
    rdp_mask = rdp([[i, int(v)] for i,v in enumerate(value_list_short)], epsilon=epsilon, return_mask=True)
    return get_short_list(value_list_short, rdp_mask, convert_to_str=False), get_short_list(key_list_short, rdp_mask, convert_to_str=False)

def remove_random(value_list_short_i, key_list_short_i, max_number_of_plot_points, min_number_of_plot_points):
    assert len(value_list_short_i) == len(key_list_short_i), "Lists value_list_short_i, key_list_short_i need to have same length"
    value_list_short = copy.deepcopy(value_list_short_i)
    key_list_short = copy.deepcopy(key_list_short_i)
    # if loop can not get right range, force it by randomly remove points between start and end
    while len(value_list_short) < min_number_of_plot_points or len(value_list_short) > max_number_of_plot_points:
        index_to_pop = random.randint(1,len(value_list_short) - 1)
        value_list_short.pop(index_to_pop)
        key_list_short.pop(index_to_pop)
    return value_list_short, key_list_short

def get_shortened_plot_data_dict(plot_data_dict, max_number_of_plot_points=20, min_number_of_plot_points=18):
    time_start = time.time()
    shortened_plot_data_dict = {}

    key_list = list(plot_data_dict.keys())
    value_list = list(plot_data_dict.values())
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
                epsilon = 0.5 + iteration_count#get_epsilon(highest_epsilon, plot_data_count, value_list_length, iteration_count)
                #print("Epsilon", epsilon, iteration_count)
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
                            value_list_short_i, key_list_short_i = remove_random(value_list_short, key_list_short, max_number_of_plot_points, min_number_of_plot_points)
                            break
                        #print(epsilon)
                        value_list_short_i, key_list_short_i = shorten_by_rdp(value_list_short, key_list_short, epsilon)
                        
                        iteration_count_internal = iteration_count_internal + 1
                        # make sure while loop does not take endless time
                        if len(value_list_short_i) >= max_number_of_plot_points:
                            value_list_short_i, key_list_short_i = remove_random(value_list_short_i, key_list_short_i, max_number_of_plot_points, min_number_of_plot_points)
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

    #print("elapsed time %.2f" % (time.time() - time_start))
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
        for plot_key in ["bsr", "prices"]:
            plot_dict = {}
            for year in sub_collection_dict["plot_data"].keys():
                if plot_key in sub_collection_dict["plot_data"][year]:
                    plot_dict.update(sub_collection_dict["plot_data"][year][plot_key])
            shortened_plot_data_dict = get_shortened_plot_data_dict(plot_dict, max_number_of_plot_points=max_number_of_plot_points, min_number_of_plot_points=min_number_of_plot_points)
            #if plot_key == "bsr":
            #    print(len(shortened_plot_data_dict.keys()))
            shortened_plot_data[plot_key + "_short"] = shortened_plot_data_dict
        return shortened_plot_data
    else:
        return shortened_plot_data
