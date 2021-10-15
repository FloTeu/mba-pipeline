import requests
import subprocess
from mwfunctions.cloud.auth import get_oauth2_id_token_by_url, get_id_token_header, get_service_account_id_token
from contextlib import suppress

MBA_PIPELINE_AI_API = "https://mw-ai-api-fidkqci7eq-ey.a.run.app"
PROJECTOR_CLOUD_RUN_MARKETPLACE2URL = {"de": "https://merchwatch-projector-de-mhttow5wga-ey.a.run.app",
                                       "com": "https://merchwatch-projector-com-mhttow5wga-ey.a.run.app"}

def get_auth_headers(endpoint_url):
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    id_token = get_oauth2_id_token_by_url(endpoint_url)
    auth_header = get_id_token_header(id_token)
    headers.update({"Authorization": auth_header})
    return headers


# descriptor fns
def update_descriptor_json_files(model_gs_url, sortby_list=["trend_nr", "bsr_last"], marketplace_list=["com","de"], update_top_n=1000, batch_size=32):
    endpoint = "/update_ai_descriptor_jsons"
    for marketplace in marketplace_list:
        for sortby in sortby_list:
            try:
                r = requests.get(f"{MBA_PIPELINE_AI_API}{endpoint}?model_gs_url={model_gs_url}&marketplace={marketplace}&sort_by={sortby}&batch_size={batch_size}&limit={update_top_n}", headers=get_auth_headers(MBA_PIPELINE_AI_API + endpoint),
                              timeout=60*20)
                print(r.text)
            except Exception as e:
                print("ERROR during update_descriptor_json_files()",str(e))


## projector fns
def update_projector_files(model_gs_url):
    endpoint = "/update_projector_files"
    try:
        r = requests.get(f"{MBA_PIPELINE_AI_API}{endpoint}?model_gs_url={model_gs_url}", headers=get_auth_headers(MBA_PIPELINE_AI_API + endpoint),
                      timeout=60*60)
        print(r.text)
    except Exception as e:
        print("ERROR during update_projector_files()",str(e))


def deploy_projector_cloud_run(marketplace):
    process = subprocess.Popen(
        f'/usr/bin/gcloud run deploy merchwatch-projector-{marketplace} --image eu.gcr.io/merchwatch/merchwatch-projector-{marketplace} --allow-unauthenticated --platform managed --project merchwatch --region="europe-west3" --service-account="merchwatch-backend@merchwatch.iam.gserviceaccount.com"  --timeout=15m --memory=1Gi',
        shell=True, stdout=subprocess.PIPE)
    process.wait()
    ping_projector_cloud_run(marketplace)

def ping_projector_cloud_run(marketplace):#
    """
        projector request must be with browser (selenium), because otherwise no interaction bewteen client and projector plugin is possible
        selenium notes:
            1. download geckodriver:
                wget https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz
                tar xvfz geckodriver-v0.30.0-linux64.tar.gz
            2. Search for bin folder in path by locking into echo $PATH and move geckofriver to bin folder:
                sudo mv geckodriver /usr/local/bin
            3. download selenium
                pip3 install selenium==4.0.0

            selenium version 4.0.0 is compatable with geckodriver 0.30.0
            compatability: https://firefox-source-docs.mozilla.org/testing/geckodriver/Support.html#supported-platforms
        https://realpython.com/modern-web-automation-with-python-and-selenium/
    """

    with suppress(Exception):
        from selenium.webdriver import Firefox
        from selenium.webdriver.firefox.options import Options
        opts = Options()
        opts.set_headless()
        assert opts.headless  # Operating in headless mode
        browser = Firefox(options=opts)
        browser.get(f"{PROJECTOR_CLOUD_RUN_MARKETPLACE2URL[marketplace]}/#mw_research")
