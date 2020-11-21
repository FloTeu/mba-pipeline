from data_handler import DataHandler
from firestore_handler import Firestore
import requests
import argparse
import time
import datetime

def get_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--marketplace',
        type=str,
        default="de",
        help='MBA marketplace')
    parser.add_argument(
        '--chunk_size',
        type=int,
        default=500,
        help='MBA marketplace')
    parser.add_argument(
        '--dev',
        type=str2bool, nargs='?',
        const=True,
        default="False",
        help='Wheter development or productive')
    parser.add_argument(
        '--update_all',
        type=str2bool, nargs='?',
        const=True,
        default="False",
        help='Wheter firestore and datastore should be completly updated')

    if argv != None:
        args, pipeline_args = parser.parse_known_args(argv)
        # args = parser.parse_args(argv)
    else:
        args, pipeline_args = parser.parse_known_args()
    return args, pipeline_args

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def send_msg(target, msg, api_key):
    """
    Send a msg to an open conversation in telegram.

    :param msg: A string. There are problems with special characters...
    :return:
    """
    bot_token = api_key
    bot_chatID = target
    # Format right
    msg = msg.replace('_', '\\_')
    send_text = 'https://api.telegram.org/bot' + bot_token + \
        '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + msg
    try:
        response = requests.get(send_text)
        return response.json()
    except:
        print("Telegram massage could not be sended.")
        return ""


def main(args):
    marketplace = args.marketplace
    time_start = time.time()

    send_msg("869595848", "Cron Job start for marketplace " + marketplace,"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")
    elapsed_time = "%.2f" % ((time.time() - time_start) / 60)
    try:
        today_day = datetime.datetime.now().day
        DataHandlerModel = DataHandler()
        DataHandlerModel.update_bq_shirt_tables(marketplace, chunk_size=args.chunk_size, dev=args.dev)
        DataHandlerModel.update_datastore(marketplace, marketplace + "_shirts", dev=args.dev, update_all=args.update_all)
        DataHandlerModel.update_firestore(marketplace, marketplace + "_shirts", dev=args.dev, update_all=args.update_all)
        # niches are updated once a week
        if today_day in [1,7,15,23]:
            send_msg("869595848", "Update niches of day %s" % today_day,"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")
            DataHandlerModel.update_niches(marketplace, chunk_size=args.chunk_size, dates=[]) #"2020-10-07", "2020-10-15", "2020-10-23", "2020-11-07", "2020-11-15"
        #DataHandlerModel.update_trademark(marketplace)

    except Exception as e:
        send_msg("869595848", str(e),"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")

    elapsed_time = "%.2f" % ((time.time() - time_start) / 60)
    send_msg("869595848", "Cron Job finished for marketplace {} | elapsed time: {} minutes".format(marketplace, elapsed_time),"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")

if __name__ == "__main__":
    # execute only if run as a script
    import sys
    args, pipeline_args = get_args(sys.argv[1:])
    main(args)
