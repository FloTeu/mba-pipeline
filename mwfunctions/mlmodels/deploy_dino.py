
from mwfunctions.cloud.aip import AI_Model
from mwfunctions import environment

project_id = environment.get_gcp_project()
region = "europe-west1"
machine_type = "n1-standard-4"
acceleratorConfig = None
acceleratorConfig = {
    'count': 1,
    'type': "NVIDIA_TESLA_K80"
}

model = AI_Model(mlflow_url, region, project_id)
model.create_model()
model.create_version(exporter_name="exporter", machineType=machine_type, acceleratorConfig=acceleratorConfig)
