#!/bin/sh
cd home/
git clone https://github.com/Flo95x/mba-pipeline.git
pip3 install -r /home/mba-pipeline/crawler/mba/requirements.txt 
cd mba-pipeline/crawler/mba/
sudo mkdir data
sudo chmod 777 data/
/usr/bin/python3 /home/mba-pipeline/crawler/mba/wc_mba_detail_daily.py de --telegram_api_key 1266137258:AAH1Yod2nYYud0Vy6xOzzZ9LdR7Dvk9Z2O0 --telegram_chatid 869595848 --number_products 50 --connection_timeout 10.0 --time_break_sec 90 --seconds_between_crawl 30 --preemptible_code thread-8-europe-west4-a --pre_instance_name mba-de-detail-pre-8 --zone europe-west4-a
    