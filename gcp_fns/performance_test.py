import requests
import time

counter = 0 
while True:
    counter = counter + 1
    time_start = time.time()
    while (time.time() - time_start) < 0.5:
        time.sleep(0.2)
    url = "http://merchwatch.de/?sort_by=trend&page=10"
    r = requests.get(url)
    print("Done: "+str(counter))

