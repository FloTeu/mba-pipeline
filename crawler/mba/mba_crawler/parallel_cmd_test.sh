for cmd in "python3 change_spider_settings.py de" "python3 change_spider_settings.py com"; do
    eval ${cmd} &
    sleep 5
done