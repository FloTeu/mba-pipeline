from bs4 import BeautifulSoup
import requests 
from requests_html import HTMLSession
import pandas as pd
import argparse
import sys
import urllib.parse as urlparse
from urllib.parse import urlencode
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import mechanize

# initialize an HTTP session
session = HTMLSession()

def get_all_forms(url):
    """Returns all form tags found on a web page's `url` """
    # GET request
    res = session.get(url)
    # for javascript driven website
    # res.html.render()
    soup = BeautifulSoup(res.html.html, "html.parser")
    return soup.find_all("form")

def get_form_details(form):
    """Returns the HTML details of a form,
    including action, method and list of form controls (inputs, etc)"""
    details = {}
    # get the form action (requested URL)
    action = form.attrs.get("action").lower()
    # get the form method (POST, GET, DELETE, etc)
    # if not specified, GET is the default in HTML
    method = form.attrs.get("method", "get").lower()
    # get all form inputs
    inputs = []
    for input_tag in form.find_all("input"):
        # get type of input form control
        input_type = input_tag.attrs.get("type", "text")
        # get name attribute
        input_name = input_tag.attrs.get("name")
        # get the default value of that input tag
        input_value =input_tag.attrs.get("value", "")
        # add everything to that list
        inputs.append({"type": input_type, "name": input_name, "value": input_value})
    # put everything to the resulting dictionary
    details["action"] = action
    details["method"] = method
    details["inputs"] = inputs
    return details

def get_main_url(marketplace):
    if marketplace == "com":
        return "https://www.amazon.com/s"
    if marketplace == "uk":
        return "https://www.amazon.co.uk/s"
    else:
        return "https://www.amazon.de/s"

def get_hidden_keywordys(marketplace):
    if marketplace == "com":
        return "Solid%3A+colors%3A+100%3A+Cotton%3A+Heather%3A+Grey%3A+90%3A+Cotton%3A+10%3A+Polyester%3A+All+Other+Heathers%3A+Classic%3A+Fit%3A+-Sweatshirt"
    if marketplace == "uk":
        return "Solid+colors+100+Cotton+Heather+Grey+90+Cotton+10+Polyester+All+Other+Heathers+Classic+Fit+-Sweatshirt"
    else:
        return 'Unifarben 100 Baumwolle Grau meliert Baumwolle -Langarmshirt'

def get_sort_statement(sort):
    "best_seller", "price_up", "price_down", "cust_rating", "oldest", "newest"
    if sort == "best_seller":
        return ""
    elif sort == "price_up":
        return "price-asc-rank"
    elif sort == "price_down":
        return "price-desc-rank"
    elif sort == "cust_rating":
        return "review-rank"
    elif sort == "oldest":
        return "-daterank"
    elif sort == "newest":
        return "date-desc-rank"
    else:
        raise("sort statement is not known!")

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('keyword', help='Keyword that you like to query in mba', type=str)
    parser.add_argument('marketplace', help='Shortcut of mba marketplace. I.e "com" or "de", "uk"', type=str)
    parser.add_argument('pod_product', help='Name of Print on Demand product. I.e "shirt", "premium", "longsleeve", "sweatshirt", "hoodie", "popsocket", "kdp"', type=str)
    parser.add_argument('sort', help='What kind of sorting do you want?. I.e "best_seller", "price_up", "price_down", "cust_rating", "oldest", "newest"', type=str)

    # get all arguments
    args = parser.parse_args()
    keyword = args.keyword
    marketplace = args.marketplace
    pod_product = args.pod_product
    sort = args.sort


    url = get_main_url(marketplace)

    hidden = get_hidden_keywordys(marketplace)
    #hidden = hidden.replace("+", "%2B")

    # rh set articles to prime
    params = {'i':'clothing','k':keyword,'s':get_sort_statement(sort), 'rh':'p_76%3A419122031%2Cp_6%3AA3JWKAKR8XB7XF', 'bbn':'77028031','hidden-keywords':get_hidden_keywordys(marketplace)}

    url_parts = list(urlparse.urlparse(url))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    query.update(params)

    url_parts[4] = urlencode(query)

    print(urlparse.urlunparse(url_parts))
  

if __name__ == '__main__':
    main(sys.argv)

