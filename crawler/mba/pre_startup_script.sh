#!/bin/sh
cd home/
git clone https://github.com/Flo95x/mba-pipeline.git
pip3 install -r /home/mba-pipeline/crawler/mba/requirements.txt 
cd mba-pipeline/crawler/mba/
sudo chmod 777 data/mba_detail_page.html
/usr/bin/python3 /home/mba-pipeline/crawler/mba/wc_mba_detail.py de --number_products 5 --connection_timeout 10.0 --time_break_sec 60 --seconds_between_crawl 20 --preemptible_code thread-2 --pre_instance_name mba-de-detail-pre-2
    