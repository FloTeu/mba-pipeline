for cmd in "python3 change_spider_settings.py de" "python3 change_spider_settings.py com" "scrapy crawl mba_general_de -a marketplace=com -a daily=False"; do
    eval ${cmd} &
    echo ${cmd}
    sleep 5
done
wait
for cmd in "python3 change_spider_settings.py de"; do
    eval ${cmd} &
    echo ${cmd}
    sleep 5
done