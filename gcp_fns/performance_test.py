import requests
import time

counter = 0 
while True:
    counter = counter + 1
    time_start = time.time()
    #while (time.time() - time_start) < 0.5:
    #    time.sleep(0.2)
    url = "http://merchwatch.de/de/about/"
    r = requests.get(url)
    print("Done: %s with status code %s in %.2f seconds" %(str(counter),r.status_code,(time.time() - time_start)))

