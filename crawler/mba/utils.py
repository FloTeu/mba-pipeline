import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import pandas_gbq
import requests 
from lxml.html import fromstring
import random 
from re import findall
from bs4 import BeautifulSoup

pd.options.mode.chained_assignment = None 
client = bigquery.Client()

def get_df_hobbies(language):
    if language == "de":
        df = pandas_gbq.read_gbq("SELECT * FROM keywords.de_hobbies", project_id="mba-pipeline")
    else:
        df = pandas_gbq.read_gbq("SELECT * FROM keywords.en_hobbies", project_id="mba-pipeline")
    return df


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project='mba-pipeline')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def make_df_of_table(soup_table_rows, columns):
    l = []
    for tr in soup_table_rows:
        td = tr.find_all('td')
        row = [tr.text for tr in td]
        l.append(row)
    return pd.DataFrame(l, columns=columns)

def is_valid_proxy(df_row, countries,https_only):
    if https_only:   
        if df_row["anonymity"] in ["anonymous", "elite proxy"] and df_row["code"] in [country.upper() for country in countries] and df_row["https"] == "yes":
            return True
        else:
            return False
    else:
        if df_row["anonymity"] in ["anonymous", "elite proxy"] and df_row["code"] in [country.upper() for country in countries]:
            return True
        else:
            return False
    

def get_proxies_sslproxies(countries,https_only):
    print("Start crawling free proxies of https://www.sslproxies.org/")
    r = requests.get('https://www.sslproxies.org/')
    '''
    matches = findall(r"<td>\d+\.\d+\.\d+\.\d+</td><td>\d+</td><td>[A-Z]{2}</td>", r.text)
    matches = [match for match in matches if match.split("<td>")[-1].split("</td>")[0] in [country.upper() for country in countries] ]
    revised = [m.replace('<td>', '') for m in matches]
    # remove country column
    sockets = [s.split('</td>')[0] + ":" + s.split('</td>')[1] for s in revised]
    return sockets
    '''
    soup = BeautifulSoup(r.text, 'html.parser')
    table_rows = soup.find("table").find_all('tr')
    df_proxies = make_df_of_table(table_rows,["IP", "port", "code", "country","anonymity", "google", "https", "last_checked"])
    df_proxies_filter = df_proxies[df_proxies.apply(lambda x: is_valid_proxy(x, countries,https_only), axis=1)]
    df_proxies_filter["proxy"] = df_proxies_filter.apply(lambda x: x["IP"] + ":" + x["port"], axis=1)
    print("Got %s available proxies" % len(df_proxies_filter))

    return df_proxies_filter["proxy"].tolist(), df_proxies_filter["code"].tolist()


def get_proxies_freeproxies(countries, https_only):
    print("Start crawling free proxies of https://free-proxy-list.net/")
    r = requests.get('https://free-proxy-list.net/')
    soup = BeautifulSoup(r.text, 'html.parser')
    table_rows = soup.find("table").find_all('tr')
    df_proxies = make_df_of_table(table_rows,["IP", "port", "code", "country","anonymity", "google", "https", "last_checked"])
    df_proxies_filter = df_proxies[df_proxies.apply(lambda x: is_valid_proxy(x, countries,https_only), axis=1)]
    df_proxies_filter["proxy"] = df_proxies_filter.apply(lambda x: x["IP"] + ":" + x["port"], axis=1)
    print("Got %s available proxies" % len(df_proxies_filter))
    return df_proxies_filter["proxy"].tolist(), df_proxies_filter["code"].tolist()

def get_proxies_with_country(countries=["de"], https_only=True):
    '''
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    if https_only:
        condition_str = './/td[7][contains(text(),"yes")]'
    else:
        condition_str = True
    for i in parser.xpath('//tbody/tr')[:400]:
        if condition_str == True or i.xpath(condition_str):
            if country == "de" and i.xpath('.//td[3]/text()')[0] == "DE" and i.xpath('.//td[5]/text()')[0] not in ["transparent", "anonymous"]:
                #Grabbing IP and corresponding PORT
                proxy=":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
            if country == "com" and i.xpath('.//td[3]/text()')[0] == "US" and i.xpath('.//td[5]/text()')[0] not in ["transparent", "anonymous"]:
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
            if country=="uk" and i.xpath('.//td[3]/text()')[0] == "GB" and i.xpath('.//td[5]/text()')[0] not in ["transparent", "anonymous"]:
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
    '''
    proxies_ssl, countries_ssl = get_proxies_sslproxies(countries,https_only)
    proxies_free, countries_free = get_proxies_freeproxies(countries,https_only)
    proxies_country = [proxie + "," + countries_ssl[i] for i, proxie in enumerate(proxies_ssl)] + [proxie + "," + countries_free[i] for i, proxie in enumerate(proxies_free)] 
    proxies_country = list(set(proxies_country))
    proxies = [item.split(",")[0] for item in proxies_country]
    countries = [item.split(",")[1] for item in proxies_country]
    print("Got %s available unique proxies" % len(proxies))
    return proxies, countries

def get_random_user_agent():
    user_agent_list = [
   #Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
    ]
    #user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    return random.choice(user_agent_list)

def get_random_headers(marketplace):
    # TODO make it realy random with trustful user agenta (should be up to date)
    headers = {
        'HOST': 'www.amazon.' + marketplace,
        'authority': 'www.amazon.' + marketplace,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'cookie': '__utma=12798129.504353392.1590337669.1590337669.1590337669.1; __utmc=12798129; __utmz=12798129.1590337669.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utmb=12798129.1.10.1590337669',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'sec-fetch-user': '?1',
        'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',}
    return headers
