
from mwfunctions.cloud.aip import AI_Model
from mwfunctions.cloud.storage import StorageParams, read_json_as_dict
import json

gs_url = "gs://5c0ae2727a254b608a4ee55a15a05fb7/ai/models/pytorch_pre_dino"

project_id = "mba-pipeline"
region = "europe-west1"
machine_type = "n1-standard-4"
acceleratorConfig = None
gpu = "NVIDIA_TESLA_K80"
gpu = False
if gpu:
    acceleratorConfig = {
        'count': 1,
        'type': gpu
    }

model = AI_Model(gs_url, region=region, project_id=project_id)

#model.create_model(build_local_only=False)
model.create_version(exporter_name="exporter", machineType=machine_type, acceleratorConfig=acceleratorConfig, wait_until_finished=True)

