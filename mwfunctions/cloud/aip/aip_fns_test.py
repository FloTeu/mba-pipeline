
from mwfunctions.cloud.aip import AI_Model
from mwfunctions.cloud.storage import StorageParams, read_json_as_dict
import json

gs_url = "gs://5c0ae2727a254b608a4ee55a15a05fb7/ai/models/pytorch_pre_dino"

#print(gs_url.split("/")[-1])
#print("{}".format(read_json_as_dict(gs_url).get("aip_model_endpoint","")))
#print("{0}/v1/{1}:predict".format(read_json_as_dict(gs_url).get("aip_model_endpoint", ""),read_json_as_dict(gs_url).get("aip_model_str", "")))

model = AI_Model(gs_url, region="europe-west1", project_id="mba-pipeline")

gpu = False
machine_type = "n1-standard-4"
acceleratorConfig = None
if gpu:
    acceleratorConfig = {
        'count': 1,
        'type': gpu
    }

#model.create_model(build_local_only=False)
model.create_version(exporter_name="exporter", machineType=machine_type, acceleratorConfig=acceleratorConfig)
