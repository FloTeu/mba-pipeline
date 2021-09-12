
from google.cloud import storage
from google.cloud.storage.blob import Blob
from typing import Optional, Union, Iterator, Tuple, List, BinaryIO
from functools import partial

import json

from mwfunctions.exceptions import log_suppress, log_if_except
import mwfunctions.misc

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


def read_file(path) -> BinaryIO:
    """ Reads file from local or gcs. Returns a file pointer (fp). You need to call .read() to get the bytes """
    import io
    import requests
    if path[:5] == "gs://":
        file_bytes = read_file_as_bytes(path)
        fp = io.BytesIO(file_bytes)
    elif path[:4] == "http":
        fp = requests.get(path, stream=True).raw
    else:
        fp = open(path, "rb")

    return fp

def read_file_as_bytes(gs_url,
             max_retries: int = 2,
             suppress_exception=False):
    """ Downloads from gs_url returns a bytes object for a file.
    """
    error_filepaths = []
    blob = Blob.from_string(gs_url, client=GCS_CLIENT)
    except_context = log_suppress if suppress_exception else log_if_except

    fn = partial(blob.download_as_bytes, client=GCS_CLIENT)
    with except_context("Could not download: {}".format(gs_url), Exception):
        return mwfunctions.misc.do_retry_if_exeption(fn, max_retries=max_retries)

    download_paths = [blob.name for blob in blobs]
    return download_paths, error_filepaths


def upload_filebytes(bucket, file_path, file_bytes,
                     content_type=None, max_retries=10):
    """
    bucket: a bucket object from gc
    file_path: path without the bucket_name

    """
    # Create a blob
    blob = bucket.blob(blob_name=file_path)
    i = 0
    while True:
        try:
            blob.upload_from_string(data=file_bytes, content_type=content_type)
            return blob.time_created, blob.size
        except Exception as e:
            if i == max_retries:
                LOGGER.critical("Could not upload: %s", file_path)
                exit(-1)
            LOGGER.error("Could not upload, trying again")
            i += 1
            time.sleep(0.1)

