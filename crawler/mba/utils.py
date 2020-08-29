import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import pandas_gbq
import requests 
from lxml.html import fromstring
import random 
from re import findall
from bs4 import BeautifulSoup
import argparse
import os 

pd.options.mode.chained_assignment = None 
client = bigquery.Client()

def get_df_hobbies(language):
    if language == "de":
        df = pandas_gbq.read_gbq("SELECT * FROM keywords.de_hobbies", project_id="mba-pipeline")
    else:
        df = pandas_gbq.read_gbq("SELECT * FROM keywords.en_hobbies", project_id="mba-pipeline")
    return df

def does_table_exist(project_id, dataset_id, table_id):
  try:
    df = client.query("SELECT * FROM %s.%s.%s" %(project_id, dataset_id, table_id)).to_dataframe().drop_duplicates()
    return True
  except Exception as e:
    return False

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
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36', # leaded to 503
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36', #  leaded 3. times to 503
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)', # leaded 2. times to 503 (might remove)
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko', # lead to 503
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)', # worked, but lead also to lead to 503
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)' # lead to 503
    ]
    #user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    return random.choice(user_agent_list)

def get_random_headers(marketplace):
    # TODO make it realy random with trustful user agenta (should be up to date)
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"
    user_agent = "Mozilla/5.0"
    headers = {
        'HOST': 'www.amazon.' + marketplace,
        'authority': 'www.amazon.' + marketplace,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        #'cookie': '__utma=12798129.504353392.1590337669.1590337669.1590337669.1; __utmc=12798129; __utmz=12798129.1590337669.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utmb=12798129.1.10.1590337669',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        #'user-agent': get_random_user_agent() #
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
        ,
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'sec-fetch-user': '?1',
        'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7'}
    return headers

def get_zone_of_marketplace(marketplace, max_instances_of_zone=4, number_running_instances=0, region_space=1):
    zone = ""
    possible_zones = {"Frankfurt":"europe-west3-a","Niederlande":"europe-west4-a","Zürich":"europe-west6-a","Belgien":"europe-west1-b","London":"europe-west2-b", "Irland":"europe-north1-a"}
    region_space_dict = {1: [possible_zones["Frankfurt"],possible_zones["Zürich"]], 2:[possible_zones["London"],possible_zones["Irland"]], 3:[possible_zones["Niederlande"],possible_zones["Belgien"]]}
    if marketplace == "de":
        if number_running_instances < max_instances_of_zone:
            zone = region_space_dict[region_space][0]
        elif number_running_instances >= max_instances_of_zone and number_running_instances < max_instances_of_zone*2:
            zone = region_space_dict[region_space][1]
        elif number_running_instances >= max_instances_of_zone*2 and number_running_instances < max_instances_of_zone*3:
            zone = possible_zones["Irland"]
            '''
            elif number_running_instances >= max_instances_of_zone*2 and number_running_instances < max_instances_of_zone*3:
                zone = "europe-west3-c"
            elif number_running_instances >= max_instances_of_zone*3 and number_running_instances < max_instances_of_zone*4:
                zone = "europe-west6-a"
            elif number_running_instances >= max_instances_of_zone*4 and number_running_instances < max_instances_of_zone*5:
                zone = "europe-west6-b"
            elif number_running_instances >= max_instances_of_zone*5 and number_running_instances < max_instances_of_zone*6:
                zone = "europe-west6-c"
            '''
        else:
            assert False, "Quota limit exceed. You cant get more instances"
    # TODO implement other cases
    else:
        print("Marketplace is not fully implemented")
        assert False, "Marketplace is not fully implemented"

    return zone

def get_div_in_html(html_str, div_class_or_id):
    html_for_bs = ""
    count_div = 0
    start_saving_html = False
    html_tag = ""
    start_get_tag = False
    html_tag_finished = ""
    
    assert not "captcha" in html_str.lower(), "Captcha is requested. Crawling will be stoped"

    for char in html_str:
        
        if div_class_or_id in html_tag and char == ">":
            start_saving_html = True
        
        html_tag = html_tag + char
        if char == "<":
            start_get_tag = True
        if char == ">":
            html_tag_finished = html_tag
            start_get_tag = False
            html_tag = ""
        
        # if div is opening div count is increasing by one
        if "<div" in html_tag_finished and start_saving_html:
            count_div += 1
        # if div is opening div count is decreasing by one
        if "</div" in html_tag_finished and start_saving_html:
            count_div -= 1
        # as long as initial parent div is not closed we fill out html str  
        if start_saving_html and html_for_bs == "":
            html_for_bs += html_tag_finished
        elif start_saving_html:
            html_for_bs += char

        # Breaking condition if closing div is reached
        if start_saving_html and count_div == 0:
            html_for_bs = html_for_bs[1:len(html_for_bs)]
            break

        html_tag_finished = ""
    
    assert html_for_bs != "", "HTML does not contains: " + div_class_or_id

    return html_for_bs

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def get_extrenal_ip(pre_instance_name, zone):
    bashCommand = "yes Y | gcloud compute instances describe {} --zone {}  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'".format(pre_instance_name, zone)
    stream = os.popen(bashCommand)
    ip_address = stream.read()
    return ip_address.replace("\n", "")

def stop_instance(pre_instance_name, zone, msg=None, api_key=None, chat_id=None):
    if msg!=None and api_key!=None and chat_id != None:
        ip_adress = get_extrenal_ip(pre_instance_name, zone)
        send_msg(chat_id, "Instance {} is stopped | IP: {} | Reason: {}".format(pre_instance_name, str(ip_adress), msg), api_key)
    bashCommand = "yes Y | gcloud compute instances stop {} --zone {}".format(pre_instance_name, zone)
    stream = os.popen(bashCommand)
    output = stream.read()
    
def send_msg(target, msg, api_key):
    """
    Send a msg to an open conversation in telegram.

    :param msg: A string. There are problems with special characters...
    :return:
    """
    bot_token = api_key
    bot_chatID = target
    # Format right
    msg = msg.replace('_', '\\_')
    send_text = 'https://api.telegram.org/bot' + bot_token + \
        '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + msg
    try:
        response = requests.get(send_text)
        return response.json()
    except:
        print("Telegram massage could not be sended.")
        return ""

def get_bsr_infos(soup_tag):
    soup_bsr = soup_tag.find(id="SalesRank")
    mba_bsr_str = [""]
    mba_bsr = [0]
    array_mba_bsr = []
    array_mba_bsr_categorie = []
    try:
        mba_bsr_str = [soup_bsr.get_text().replace("\n", "")]
        bsr_iterator = mba_bsr_str[0].split("Nr. ")
        bsr_iterator = bsr_iterator[1:len(bsr_iterator)]
        for bsr_str in bsr_iterator:
            bsr = int(bsr_str.split("in")[0].replace(".", ""))
            array_mba_bsr.append(bsr)
            bsr_categorie = bsr_str.split("(")[0].split("in")[1].replace("\xa0", "").strip()
            array_mba_bsr_categorie.append(bsr_categorie)
        mba_bsr = [int(bsr_iterator[0].split("in")[0].replace(".", ""))]
    except:
        raise ValueError
        pass

    return mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie

def get_customer_review_infos(soup_tag):
    soup_review = soup_tag.find(id="detailBullets_averageCustomerReviews")
    customer_recession_score_mean = [0.0]
    customer_recession_score = [""]
    customer_recession_count = [0]
    try:
        try:
            customer_recession_score = [soup_review.find("span", class_="a-declarative").find("a").find("i").get_text()]
        except:
            customer_recession_score = [""]
        try:
            customer_recession_count = [int(soup_review.find("a", id="acrCustomerReviewLink").get_text().split(" ")[0])]
        except:
            customer_recession_count = [0]
        try:
            customer_recession_score_mean = [float(customer_recession_score[0].split(" von")[0].replace(",","."))]
        except:
            customer_recession_score_mean = [0.0]
    except:
        pass

    return customer_recession_score_mean, customer_recession_score, customer_recession_count

def get_weight_infos(soup_tag):
    weight = ["Not found"]
    li_list = soup_tag.find_all("li")
    for li in li_list:
        info_text = li.get_text().lower()
        if "gewicht" in info_text or "weight" in info_text or "abmessung" in info_text:
            weight = [li.get_text()]
    return weight


def get_upload_date_infos(soup_tag):
    upload_date = ["Not found"]
    li_list = soup_tag.find_all("li")
    for li in li_list:
        info_text = li.get_text().lower()
        if "seit" in info_text or "available" in info_text:
            upload_date = [li.get_text()]
    return upload_date