import json
import random
import numpy as np
from .utils import get_proxies_with_country
import pathlib
from mwfunctions.pydantic.security_classes import MWSecuritySettings

list_country_proxies = ["DE", "dk", "pl", "fr", "ua", "us", "cz"]

def get_random_proxy(path_proxy_json, only_working=False):
    with open(path_proxy_json) as json_file:
        json_proxies = json.load(json_file)

    json_proxies_working = []
    if only_working:
        for proxy in json_proxies:
            if "working" in proxy.keys() and proxy["working"]:
                json_proxies_working.append(proxy)
    else:
        json_proxies_working = json_proxies

    random_choice = random.choice(np.arange(len(json_proxies_working)))
    return json_proxies_working[random_choice]

def get_random_proxy_url(path_proxy_json, only_working=False):
    raise NotImplementedError
    # url = get_random_proxy(path_proxy_json, only_working=only_working)["Url"] + ":3128"

def get_random_proxy_url_dict(path_proxy_json=None, only_working=False):
    path_proxy_json = path_proxy_json if path_proxy_json else f'{pathlib.Path(__file__).parent.resolve()}/proxies.json'
    proxy_url_http, proxy_url_https = get_random_proxy_url(path_proxy_json, only_working=only_working)
    proxies = {
        "http": proxy_url_http
        ,"https": proxy_url_https
    }
    return proxies

def get_private_http_proxy_list(mw_security_settings: MWSecuritySettings, only_usa):
    http_proxy_list = []
    for proxy_service_name, proxy_service_settings in mw_security_settings.proxy_services.items():
        if not proxy_service_settings.active:
            continue
        for proxy in proxy_service_settings.proxy_items:
            if not only_usa or proxy.location in ["USA", "Iceland"]:
                url = f"http://{proxy_service_settings.proxy_provider_login.user_id}:{proxy_service_settings.proxy_provider_login.password}@{proxy.url}:{proxy_service_settings.default_port}"
                http_proxy_list.append(url)
    return http_proxy_list

def get_public_http_proxy_list(only_usa):
    list_country_public_proxies = list_country_proxies
    if only_usa:
        list_country_public_proxies = ["us"]
    http_proxy_list, new_country_list = get_proxies_with_country(list_country_public_proxies, https_only=False)
    return http_proxy_list

def get_http_proxy_list(mw_security_settings: MWSecuritySettings, only_usa=False):
    private_http_proxy_list = get_private_http_proxy_list(mw_security_settings, only_usa)
    public_http_proxy_list = get_public_http_proxy_list(only_usa)
    http_proxy_list = private_http_proxy_list + public_http_proxy_list

    return http_proxy_list