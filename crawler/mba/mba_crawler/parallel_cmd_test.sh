for cmd in "sudo python3 change_spider_settings.py de" "sudo scrapy crawl mba_overview -a marketplace=de -a pod_product=shirt -a sort=best_seller -a pages=0 -a start_page=10" "sudo python3 change_spider_settings.py com" "sudo scrapy crawl mba_overview -a marketplace=com -a pod_product=shirt -a sort=best_seller -a pages=190 -a start_page=210"; do
        eval ${cmd} &
        sleep 5
    done

# for cmd in "sudo python3 change_spider_settings.py de" "sudo scrapy crawl mba_overview -a marketplace=de -a pod_product=shirt -a sort=newest -a pages=10 -a start_page=1" "sudo python3 change_spider_settings.py com" "sudo scrapy crawl mba_overview -a marketplace=com -a pod_product=shirt -a sort=newest -a pages=10 -a start_page=1"; do
#         eval ${cmd} &
#         sleep 5
#     done