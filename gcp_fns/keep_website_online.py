import requests
import time
import urllib.request
import argparse
import sys
import subprocess

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

def start_server():
    bash_command = 'cd /home/flo_t_1995/mba-pipeline/mba-page/merchwatch/ && nohup sudo python3 manage.py runserver 0.0.0.0:80'
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

def main(argv):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('interval',type=int, help='Seconds/interval in which server status is checked')
    parser.add_argument('--telegram_api_key',default="", help='API key of mba bot', type=str)
    parser.add_argument('--telegram_chatid', default="", help='Id of channel like private chat or group channel', type=str)

    # if python file path is in argv remove it 
    if ".py" in argv[0]:
        argv = argv[1:len(argv)]

    # get all arguments
    args = parser.parse_args(argv)
    interval = args.interval
    api_key = args.telegram_api_key
    chat_id = args.telegram_chatid

    counter = 0 

    while True:
        counter = counter + 1
        url = "http://merchwatch.de"
        try:
            response_code = urllib.request.urlopen(url).getcode()
            print(response_code)
        except:
            send_msg(chat_id, "Server is not online anymore. Try to start it again..",api_key)
            print("Error occured website is not online")
        time.sleep(interval)

if __name__ == '__main__':
    main(sys.argv)
