import base64
import json

def str2b64_bytes(msg_str):
    message_bytes = msg_str.encode('utf-8')
    return base64.b64encode(message_bytes)

def str2b64_str(msg_str):
    return str2b64_bytes(msg_str).decode('utf-8')

def str_list2b64_str(msg_str_list):
    json_encoded_list = json.dumps(msg_str_list)
    return str2b64_str(json_encoded_list)

def b64_str2b64_bytes(b64_str):
    return b64_str.encode('utf-8')

def b64_str2str(b64_str):
    message_bytes = base64.b64decode(b64_str2b64_bytes(b64_str))
    return message_bytes.decode('utf-8')

def b64_str2str_list(b64_str):
    decoded_list = base64.b64decode(b64_str)
    return json.loads(decoded_list)