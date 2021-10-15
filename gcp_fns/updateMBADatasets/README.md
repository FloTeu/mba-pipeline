

## Requirements for instance
### Selenium
Selenium is required send successfull requests to cloud run browser 

Installation:

projector request must be with browser (selenium), because otherwise no interaction bewteen client and projector plugin is possible
selenium notes:
* download geckodriver:
  
    ```
  wget https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz &&
  tar xvfz geckodriver-v0.30.0-linux64.tar.gz```
* Search for bin folder in path by locking into echo $PATH and move geckofriver to bin folder:
        
  ```sudo mv geckodriver /usr/local/bin```
* download selenium

  ```pip3 install selenium==4.0.0```

selenium version 4.0.0 is compatable with geckodriver 0.30.0
compatability: https://firefox-source-docs.mozilla.org/testing/geckodriver/Support.html#supported-platforms
https://realpython.com/modern-web-automation-with-python-and-selenium/

### nltk files