from bs4 import BeautifulSoup
import requests 
from requests_html import HTMLSession
import pandas as pd
import argparse
import sys
import urllib.parse as urlparse
from urllib.parse import urlencode
from urllib.parse import urljoin
from utils import get_df_hobbies
import mba_url_creator as url_creator

def main(argv):
    parser = argparse.ArgumentParser(description='')

    # get all arguments
    args = parser.parse_args()

    #df = get_df_hobbies("de")
    df = pd.read_csv("~/mba-pipeline/crawler/mba/data/hobbies_de.csv")
    hobbies_list = df["hobby"].tolist()
    test_hobby = hobbies_list[4]

    url_mba = url_creator.main(["Fischen", "de", "shirt", "newest"])

    response = requests.get(url_mba)
    soup = BeautifulSoup(response.content, 'html.parser')

    test = 0

if __name__ == '__main__':
    main(sys.argv)

