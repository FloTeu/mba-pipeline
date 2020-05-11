from bs4 import BeautifulSoup
import requests 
import pandas as pd
import argparse
import sys
import urllib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('pod_product', help='Name of Print on Demand product. I.e "shirt", "premium", "longsleeve", "sweatshirt", "hoodie", "popsocket", "kdp"', type=str)
    parser.add_argument('sort', help='What kind of sorting do you want?. I.e "best_seller", "price_up", "price_down", "cust_rating", "oldest", "newest"', type=str)

    # get all arguments
    args = parser.parse_args()
    marketplace = args.marketplace
    pod_product = args.pod_product
    sort = args.sort

    url = 'https://merchresearch.de/'
    #response = requests.get(url)
    #soup = BeautifulSoup(response.content, 'html.parser')
    #test = 0

    browser = webdriver.Chrome("/usr/bin/chromedriver") #download chromebrowser
    browser.get(url) #open page in browser
    outDF = pd.DataFrame(columns=['prodname', 'imageurl', 'minprice', 'maxprice', 'actualprice']) #template of data
    browser.find_element(By.XPATH, "//input[contains(@name, 'site_house_number_low')]").send_keys('3419')
    browser.find_element(By.XPATH, "//input[contains(@name, 'site_street_name')]").send_keys('Wabash')
    x = browser.find_elements(By.XPATH, "//button[contains(@type, 'submit')]/span")[1].click()


if __name__ == '__main__':
    main(sys.argv)

