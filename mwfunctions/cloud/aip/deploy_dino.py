
from mwfunctions.cloud.aip import AI_Model
from mwfunctions.cloud.storage import StorageParams, read_json_as_dictg
import json

gs_url = "gs://5c0ae2727a254b608a4ee55a15a05fb7/ai/models/pytorch_pre_dino"

project_id = environment.get_gcp_project()
region = "europe-west1"
machine_type = "n1-standard-4"
acceleratorConfig = None
acceleratorConfig = {
    'count': 1,
    'type': "NVIDIA_TESLA_K80"
}

model = AI_Model(gs_url, region=region, project_id="mba-pipeline")

#model.create_model(build_local_only=False)
model.create_version(exporter_name="exporter", machineType=machine_type, acceleratorConfig=acceleratorConfig)

