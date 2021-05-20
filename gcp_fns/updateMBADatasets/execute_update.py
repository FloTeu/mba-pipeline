from data_handler import DataHandler
from niche_updater import NicheUpdater, NicheAnalyser
from firestore_handler import Firestore
from bigquery_handler import BigqueryHandler
import requests
import argparse
import time
import datetime
import pytz

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
        tz = pytz.timezone('Europe/Berlin')
        today_day = datetime.datetime.now(tz).day
        today_weekday = datetime.datetime.now(tz).weekday()
        BigQueryHandlerModel = BigqueryHandler(marketplace=marketplace, dev=args.dev)
        DataHandlerModel = DataHandler(marketplace=marketplace, bigquery_handler=BigQueryHandlerModel)
        NicheUpdaterModel = NicheUpdater(marketplace=marketplace, dev=args.dev)
        try:
            from api_keys import API_KEYS
            NicheAnalyserModel = NicheAnalyser(marketplace=marketplace, dev=args.dev)
            NicheAnalyserModel.set_df()
            NicheAnalyserModel.update_fs_trend_niches()
        except Exception as e:
            print("Could not load API key", str(e))
        keywords="brawl" #"Schleich di du Oaschloch; Dezentralisierung, Wolliball, Lockdown 2021,Agrardemiker;Among;Schlafkleidung;Querdenken;Qanon"
        #NicheUpdaterModel.crawl_niches(keywords)
        #DataHandlerModel.update_niches_by_keyword(marketplace, keywords)
        #NicheUpdaterModel.update_firestore_niche_data(keywords=keywords)

        BigQueryHandlerModel.product_details_daily_data2file()
        DataHandlerModel.update_bq_shirt_tables(marketplace, chunk_size=args.chunk_size, dev=args.dev)
        DataHandlerModel.update_firestore(marketplace, marketplace + "_shirts", dev=args.dev, update_all=args.update_all)
        # niches are updated once a week every sunday
        if today_weekday == 6:
            DataHandlerModel.update_trademark(marketplace)
            send_msg("869595848", "Update niches of day %s" % today_day,"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")
            DataHandlerModel.update_language_code(marketplace)
            #DataHandlerModel.update_niches(marketplace, chunk_size=args.chunk_size, dates=[]) #2021-02-21 "2021-01-10" "2020-10-11", "2020-10-18", "2020-10-25","2020-11-01", "2020-11-22"
            NicheUpdaterModel.delete_all_niches_by_type("trend_niche")

    except Exception as e:
        send_msg("869595848", str(e),"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")

    elapsed_time = "%.2f" % ((time.time() - time_start) / 60)
    send_msg("869595848", "Cron Job finished for marketplace {} | elapsed time: {} minutes".format(marketplace, elapsed_time),"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")

if __name__ == "__main__":
    # execute only if run as a script
    import sys
    args, pipeline_args = get_args(sys.argv[1:])
    main(args)
