import os
import json
import urllib
import requests
import base64

url_local = 'http://0.0.0.0:5000'
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

with open("sample_pubsub.json", 'rb') as f:
    pubsub_dict = json.load(f)
with open("data.json", 'rb') as f:
    data_encoded = json.dumps(json.load(f)).encode()

#pubsub_json = json.dumps(pubsub_dict)
# data of pub sub is base 64 encoded but probably sended as string object
pubsub_dict["data"]["data"] = base64.encodestring(data_encoded).decode()

#url_query = urllib.parse.urlencode(data_dict, doseq=False)
url = url_local #+ "?" + url_query

r = requests.post(url, data=json.dumps(pubsub_dict), headers=headers)
#r = requests.get(url, verify=True)
print(r.text)
