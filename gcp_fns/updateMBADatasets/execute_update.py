from data_handler import DataHandler
from niche_updater import NicheUpdater, NicheAnalyser
from firestore_handler import Firestore
from bigquery_handler import BigqueryHandler
from ai_fns import update_descriptor_json_files, update_projector_files, deploy_projector_cloud_run
import merchwatch_daily_creator as merchwatch_daily_creator

import requests
import os
import argparse
import time
import datetime
import pytz
import pandas as pd

from mwfunctions.cloud.auth import get_headers_by_service_url
from mwfunctions.cloud.aip import AI_Model

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
        '--debug_limit', help='Whether only limit of asins should be used for execution', type=int, default=None)
    parser.add_argument(
        '--num_threads', help='How many threads should be used for multiprocessing', type=int, default=8)
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

def remove_blacklisted_shirts_from_firestore(marketplace):
    firestore_model = Firestore(f"{marketplace}_niches")
    df = pd.read_gbq(f"SELECT DISTINCT asin FROM mba_{marketplace}.products_no_mba_shirt where url LIKE '%amazon.{marketplace}/dp/%'", project_id="mba-pipeline")
    firestore_model.delete_by_df_batch(df, "asin", batch_size=100)


def update_bq_table(args, marketplace, BigQueryHandlerModel):
    BigQueryHandlerModel.product_details_daily_data2file()
    args_merchwatch_daily_creator = [marketplace, "--chunk_size", args.chunk_size, "--num_threads", args.num_threads]
    if args.dev:
        args_merchwatch_daily_creator.append("--dev")
    if args.debug_limit:
        args_merchwatch_daily_creator.extend(["--debug_limit", args.debug_limit])
    merchwatch_daily_creator.main([str(v) for v in args_merchwatch_daily_creator])

def main(args):
    marketplace = args.marketplace
    time_start = time.time()
    args.debug_limit = args.debug_limit if args.debug_limit != 0 else None

    send_msg("869595848", "Cron Job start for marketplace " + marketplace,"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")
    elapsed_time = "%.2f" % ((time.time() - time_start) / 60)
    try:
        tz = pytz.timezone('Europe/Berlin')
        today_day = datetime.datetime.now(tz).day
        today_weekday = datetime.datetime.now(tz).weekday()
        BigQueryHandlerModel = BigqueryHandler(marketplace=marketplace, dev=args.dev)
        DataHandlerModel = DataHandler(marketplace=marketplace, bigquery_handler=BigQueryHandlerModel)
        NicheUpdaterModel = NicheUpdater(marketplace=marketplace, dev=args.dev)
        ML_MODEL_URL = "gs://5c0ae2727a254b608a4ee55a15a05fb7/ai/models/pytorch_pre_dino"
        model = AI_Model(ML_MODEL_URL, region="europe-west1", project_id="mba-pipeline")
        keywords="brawl" #"Schleich di du Oaschloch; Dezentralisierung, Wolliball, Lockdown 2021,Agrardemiker;Among;Schlafkleidung;Querdenken;Qanon"
        #NicheUpdaterModel.crawl_niches(keywords)
        #DataHandlerModel.update_niches_by_keyword(marketplace, keywords)
        #NicheUpdaterModel.update_firestore_niche_data(keywords=keywords)

        update_bq_table(args, marketplace, BigQueryHandlerModel)
        # TODO: update FS directly via crawler not from BQ. If you want to update it via keep in mind that things like customer review count are not included. in current process.
        DataHandlerModel.update_firestore(marketplace, marketplace + "_shirts", dev=args.dev, update_all=args.update_all)
        
        # Delete every day trend niche designs but only those which are older than one week
        NicheUpdaterModel.delete_all_niches_by_type("trend_niche", days=7)
        # niches are updated once a week every sunday
        if today_weekday == 6:
            DataHandlerModel.update_trademark(marketplace)
            send_msg("869595848", "Update niches of day %s" % today_day,"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")
            DataHandlerModel.update_language_code(marketplace)
            try:
                # AI related updates
                acceleratorConfig = {
                    'count': 1,
                    'type': "NVIDIA_TESLA_K80"
                }
                model.create_version(exporter_name="exporter", machineType="n1-standard-4",
                                     acceleratorConfig=acceleratorConfig, wait_until_finished=True)
                update_descriptor_json_files(ML_MODEL_URL, sortby_list=["trend_nr", "bsr_last"], marketplace_list=["com","de"])
            except Exception as e:
                print("Could not create descriptor files", str(e))
            model.delete_version()

        if today_weekday == 6 or today_weekday == 2: #sunday or wednesday
            # TODO why does response cannot be received before timeout error?
            update_projector_files(ML_MODEL_URL)
            # redeploy cloud run. Otherwise projector might take old files or does not work correctly
            deploy_projector_cloud_run(marketplace)

            #DataHandlerModel.update_niches(marketplace, chunk_size=args.chunk_size, dates=[]) #2021-02-21 "2021-01-10" "2020-10-11", "2020-10-18", "2020-10-25","2020-11-01", "2020-11-22"

        # update trend_niches after deletion process
        try:
            from api_keys import API_KEYS
            NicheAnalyserModel = NicheAnalyser(marketplace=marketplace, dev=args.dev)
            NicheAnalyserModel.set_df()
            NicheAnalyserModel.update_fs_trend_niches()
        except Exception as e:
            print("Could not load API key", str(e))
            

    except Exception as e:
        print(str(e))
        # make sure model version is deleted
        model.delete_version()
        send_msg("869595848", str(e),"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")
        raise e

    elapsed_time = "%.2f" % ((time.time() - time_start) / 60)
    send_msg("869595848", "Cron Job finished for marketplace {} | elapsed time: {} minutes".format(marketplace, elapsed_time),"1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0")

if __name__ == "__main__":
    # execute only if run as a script
    import sys
    args, pipeline_args = get_args(sys.argv[1:])
    main(args)
