import requests
from mwfunctions.cloud.auth import get_oauth2_id_token_by_url, get_id_token_header, get_service_account_id_token

MBA_PIPELINE_AI_API = "https://mw-ai-api-fidkqci7eq-ey.a.run.app"

# descriptor fns
def update_descriptor_json_files(model_gs_url, sortby_list=["trend_nr", "bsr_last"], marketplace_list=["com","de"], update_top_n=1000, batch_size=32):
    endpoint = "/update_ai_descriptor_jsons"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    id_token = get_oauth2_id_token_by_url(MBA_PIPELINE_AI_API + endpoint)
    auth_header = get_id_token_header(id_token)
    headers.update({"Authorization": auth_header})
    for marketplace in marketplace_list:
        for sortby in sortby_list:
            try:
                r = requests.get(f"{MBA_PIPELINE_AI_API}{endpoint}?model_gs_url={model_gs_url}&marketplace={marketplace}&sort_by={sortby}&batch_size={batch_size}&limit={update_top_n}", headers=headers,
                              timeout=60*20)
                print(r.text)
            except Exception as e:
                print(str(e))


## projector fns
def update_projector_files(model_gs_url):
    endpoint = "/update_projector_files"
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    id_token = get_oauth2_id_token_by_url(MBA_PIPELINE_AI_API + endpoint)
    auth_header = get_id_token_header(id_token)
    headers.update({"Authorization": auth_header})
    r = requests.get(f"{MBA_PIPELINE_AI_API}{endpoint}?model_gs_url={model_gs_url}", headers=headers,
                      timeout=60*60)
    print(r.text)
