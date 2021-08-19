sudo /usr/bin/python3 create_url_csv.py de False --number_products=700 & sudo /usr/bin/python3 create_url_csv.py com False --number_products=500
for cmd in "sudo python3 change_spider_settings.py de" "sudo scrapy crawl mba_general_de -a marketplace=de -a daily=False" "sudo python3 change_spider_settings.py com" "sudo scrapy crawl mba_general_de -a marketplace=com -a daily=False"; do
        eval ${cmd} &
        sleep 5
    done