# This will construct the needed http request for getting data

from mwfunctions.cloud.aip import AI_Model
from mwfunctions import environment

gs_url = "gs://5c0ae2727a254b608a4ee55a15a05fb7/ai/models/pytorch_pre_dino"

project_id = environment.get_gcp_project()
region = "europe-west1"

model = AI_Model(gs_url, region=region, project_id="mba-pipeline")
model.delete_version()
