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


def main(argv):
    parser = argparse.ArgumentParser(description='')

    # get all arguments
    args = parser.parse_args()

    df = get_df_hobbies("de")
    print(df.head())

    test = 0
if __name__ == '__main__':
    main(sys.argv)

