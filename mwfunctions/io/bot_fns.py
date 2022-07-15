import requests

def send_telegram_msg(msg, target, api_key):
    """
    Send a msg to an open conversation in telegram.

    :param msg: A string. There are problems with special characters...
    :return:
    """
    # Format right
    msg = msg.replace('_', '\\_')
    send_text = 'https://api.telegram.org/bot' + api_key + \
        '/sendMessage?chat_id=' + target + '&parse_mode=Markdown&text=' + msg
    try:
        response = requests.get(send_text)
        return response.json()
    except:
        print("Telegram massage could not be sended.")
        return ""