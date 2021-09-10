
from google.cloud import storage
from google.cloud.storage.blob import Blob
import json

GCS_CLIENT = storage.Client()

class StorageParams():
    def __init__(self, gs_url, project="mba-pipeline", credentials=None):
        assert "gs://" in gs_url, "not a valid gs_url"

        self.project = project
        self.gs_url = gs_url
        self.bucket_name = self.gs_url.split("gs://")[1].split("/")[0]
        self.path = "/".join(self.gs_url.split("gs://")[1].split("/")[1:])
        self.credentials = credentials

def read_file_as_string(gs_url):
    storage_params = StorageParams(gs_url)
    bucket = GCS_CLIENT.get_bucket(storage_params.bucket_name)
    blob = bucket.get_blob(storage_params.path)
    return blob.download_as_string()

def read_json_as_dict(gs_url):
    return json.loads(read_file_as_string(gs_url))
