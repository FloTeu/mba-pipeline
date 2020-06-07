#!/bin/sh
cd home/
git clone https://github.com/Flo95x/mba-pipeline.git
pip3 install -r /home/mba-pipeline/crawler/mba/requirements.txt 
cd mba-pipeline/crawler/mba/
sudo mkdir data
sudo chmod 777 data/
/usr/bin/python3 /home/mba-pipeline/crawler/mba/wc_mba_detail.py de --number_products 30 --connection_timeout 10.0 --time_break_sec 120 --seconds_between_crawl 20 --preemptible_code thread-7-europe-west6-a --pre_instance_name mba-de-detail-pre-7 --zone europe-west6-a
    