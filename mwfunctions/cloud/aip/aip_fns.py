import os
import time
import subprocess
import socket
import requests

from pydantic import BaseModel
from google.cloud import storage
from googleapiclient import discovery
from requests_futures.sessions import FuturesSession

import mwfunctions.constants.ml_constants as mlc
from mwfunctions.cloud.auth import get_service_account_id_token, get_id_token_header
from mwfunctions.cloud.storage import read_json_as_dict
from .output_parser import AIPOutputParser

def response_to_json_hook(resp, *args, **kwargs):
    # parse the json storing the result on the response object
    resp.data = resp.json()


class AIPTfB64ImageInstance(BaseModel):
    b64: str

# List of instances which should be sended to AIP. e.g. [{"b64": base64_str}]
class AIPTfB64ImageInstancePtWrapper(BaseModel):
    """This is what ai engine expects as an instance for a pytorch model query"""
    data: AIPTfB64ImageInstance

class AI_Model():
    def __init__(self, model_gs_url, region=None, project_id='mba-pipeline', future_sessions_max_workers=2, squeeze_result=True):
        '''
        Model Class designed for use with mlflow run url.

        1. Future Request Session
        2. Authentication
        3. Error handling

        :param model_gs_url:
        :param region:
        :param project_id:
        '''
        self.model_gs_url = model_gs_url
        self.region = region
        self.project_id = project_id
        self.future_sessions_max_workers = future_sessions_max_workers
        self.meta_dict_gs_url = f"{model_gs_url}/model_meta.json"
        
        self.meta_tags_dict = read_json_as_dict(self.meta_dict_gs_url)["tags"]

        # TODO: Does this work anymore?
        # set timeout for request
        self.timeout = 60
        socket.setdefaulttimeout(self.timeout)

        self.framework = self.meta_tags_dict.get(
            mlc.ML_MODEL_FRAMEWORK_TAG, mlc.ML_MODEL_FRAMEWORK_PYTOCH)
        self.model_name = self.meta_tags_dict.get(mlc.ML_MODEL_MODEL_NAME_TAG, "")

        self.aip_model_name = 'ml_model_' + str(model_gs_url.split("/")[-1])

        # set global or regional endpoint
        self.endpoint = f'https://ml.googleapis.com' if not region else f'https://{self.region}-ml.googleapis.com'

        self.scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        self.reload_auth_headers()

        self.session = FuturesSession(
            max_workers=self.future_sessions_max_workers)  # 8 is default

        # POST https://{endpoint}/v1/{parent=projects/*}/models
        self.aip_projects_models_create_url = f"https://{self.region + '-' if self.region else ''}ml.googleapis.com/v1/projects/{self.project_id}/models"

        self.aip_inference_url = "{0}/v1/{1}:predict".format(self.meta_tags_dict.get("aip_model_endpoint", ""), self.meta_tags_dict.get("aip_model_str", ""))

        self.output_parser = AIPOutputParser(framework=self.framework, squeeze_result=squeeze_result)
        self.model_image =  f"{self.region}-docker.pkg.dev/{self.project_id}/pytorch-models/{self.aip_model_name}:latest"

    def reload_auth_headers(self):
        self.auth_headers = {'Authorization': get_id_token_header(get_service_account_id_token(
            scopes=self.scopes)), 'Content-Type': 'application/json; UTF-8'}

    def create_model(self, gcloud_build=True, num_worker=None, build_local_only=False):
        # https://cloud.google.com/ai-platform/prediction/docs/reference/rest/v1/projects.models/create
        # https://cloud.google.com/ai-platform/prediction/docs/reference/rest/v1/projects.models#Model
        print(f'creating model {self.aip_model_name}...')

        create_model_json_dict = {"name": self.aip_model_name}

        # pytorch serve container build
        if self.framework == mlc.ML_MODEL_FRAMEWORK_PYTOCH and gcloud_build:
            self.build_pytorch_serve_container(num_worker, build_local=build_local_only)

        future = self.session.post(self.aip_projects_models_create_url, json=create_model_json_dict, timeout=self.timeout, hooks={
                                   "response": response_to_json_hook}, headers=self.auth_headers)
        resp = future.result()
        print(resp.data)

    def build_pytorch_serve_container(self, num_worker, build_local=False):
        source_dir = os.path.dirname(os.path.realpath(__file__))
        # Mar file is model archive file
        gcs_mar_file = f"{self.model_gs_url}/exporter/{self.model_name}.mar"
        build_yaml = f"{source_dir}/pytorch_cloudbuild.yaml"
        assert os.path.exists(build_yaml), "Could not find cloud_build yaml"


        if not build_local:
            # no-cache not working with config
            args = ["gcloud", "builds", "submit", source_dir,
                    f"--config={build_yaml}",
                    f"--project={self.project_id}",
                    f"--timeout=30m",
                    f'--substitutions=_PROJECT_ID={self.project_id},'
                    f'_CLOUD_STORAGE_PATH={gcs_mar_file},'
                    f'_AIP_MODEL_NAME={self.aip_model_name},'
                    f'_NUM_WORKER={num_worker},'
                    f'_MODEL_NAME={self.model_name},'
                    f'_REGION={self.region}']
        else:
            args = ["cloud-build-local", 
                    f"--config={build_yaml}",
                    f"--dryrun=False",
                    f'--substitutions=_PROJECT_ID={self.project_id},'
                    f'_CLOUD_STORAGE_PATH={gcs_mar_file},'
                    f'_AIP_MODEL_NAME={self.aip_model_name},'
                    f'_NUM_WORKER={num_worker},'
                    f'_MODEL_NAME={self.model_name},'
                    f'_REGION={self.region}',
                    source_dir]
        # quickfix to make gcloud work again
        # TODO: check if deployment works on cloud instance
        if "google-cloud-sdk" not in os.environ["PATH"]:
            os.environ["PATH"] = os.environ["PATH"]  + ':/home/fteutsch/miniconda3/envs/exports/google-cloud-sdk/bin'
        subprocess.run(args, check=True)
        #process = subprocess.Popen(args, stdout=subprocess.PIPE)
        #output, error = process.communicate()

    # todo: add ai engine argument as **kwargs
    def create_version(self, exporter_name: str = "exporter",
                       machineType: str = "n1-standard-4",
                       acceleratorConfig=None,
                       wait_until_finished=False,
                       runtimeVersion='2.3',
                       pythonVersion='3.7',
                       min_nodes=1,
                       max_nodes=4):
        # acceleratorConfig = {
        #     'count': 1,
        #     'type': "NVIDIA_TESLA_K80"
        # }
        # https://cloud.google.com/ai-platform/prediction/docs/reference/rest/v1/projects.models.versions
        print(f'creating model version... ')
        self.reload_auth_headers()

        time_start = time.time()

        autoScaling = {"minNodes": min_nodes, "maxNodes": max_nodes}
        create_version_dict = {"machineType": machineType, "autoScaling": autoScaling}
        if self.framework == mlc.ML_MODEL_FRAMEWORK_PYTOCH:
            routes = {"health": "/ping",
                      "predict": f"/predictions/{self.meta_tags_dict[mlc.ML_MODEL_MODEL_NAME_TAG]}"}
            ports = {"containerPort": 8080}
            container_spec = {"image": f"{self.region}-docker.pkg.dev/{self.project_id}/pytorch-models/{self.aip_model_name}",
                              "ports": ports, }
            create_version_dict.update({
                # --region = REGION \
                "container": container_spec,
                "routes": routes,
                "name": self.meta_tags_dict[mlc.ML_MODEL_MODEL_NAME_TAG],
            })
        else:
            create_version_dict.update({
                "name": self.aip_model_name,
                "deploymentUri": os.path.join(self.mvflow.artifact_uri, exporter_name),
                "runtimeVersion": runtimeVersion,
                "framework": "TENSORFLOW",
                "pythonVersion": pythonVersion,
            })

        if acceleratorConfig:
            create_version_dict['acceleratorConfig'] = acceleratorConfig

        aip_project_models_versions_create_url = f"https://{self.region + '-' if self.region else ''}ml.googleapis.com/v1/projects/{self.project_id}/models/{self.aip_model_name}/versions"
        future = self.session.post(aip_project_models_versions_create_url, json=create_version_dict, timeout=self.timeout, hooks={
                                   "response": response_to_json_hook}, headers=self.auth_headers)
        resp = future.result()
        if resp.status_code == 200:
            self.aip_model_str = f'projects/{self.project_id}/models/{self.aip_model_name}'
            # Set tags
            self.meta_tags_dict["aip_model_str"] = self.aip_model_str
            # TODO upload meta data json to storage
            #self.meta_dict_gs_url()

            if wait_until_finished:
                while not self.is_ready():
                    time.sleep(30)
                print("Elapsed time until model version is ready: %.2f minutes" % ((time.time() - time_start) / 60 ))
        print(resp)
        print(resp.data)

    def deploy_local(self, gpu=None, batch_size=8):
        try:
            args_str = f"docker run --name {self.model_name} --rm {'--gpus ' + gpu  if gpu else ''} -d -p 8080:8080 -p 8081:8081 -p 8082:8082 -p 7070:7070 -p 7071:7071 {self.model_image}"
            subprocess.run(args_str,
                           shell=True, check=True,
                           executable='/bin/bash')
        except Exception as e:
            print(str(e))
        while True:
            try:
                if requests.get(f'http://0.0.0.0:8080/ping').json()["status"] != "Healthy":
                    print("Status is not healthy", requests.get(f'http://0.0.0.0:8080/ping').json()["status"])
                    # wait some seconds
                    time.sleep(5)
                else:
                    break
            except Exception as e:
                print("Error:", e)
                time.sleep(5)

        import multiprocessing
        requests.post(f'http://0.0.0.0:8081/models?url={self.model_name}.mar&model_name={self.model_name}&batch_size={batch_size}&max_batch_delay=1000&initial_workers=1')
        requests.put(f'http://0.0.0.0:8081/models/{self.model_name}?min_worker=1&max_worker={ multiprocessing.cpu_count()}&batch_size={batch_size}')
        requests.put(f'http://0.0.0.0:8081/models/{self.model_name}/1.0/set-default')
        #if gpu:
        #    requests.put(f'http://0.0.0.0:8081/models/{self.model_name}?gpu=True')

        # requests.get(f'http://0.0.0.0:8081/models/{self.model_name}').text
        # args_str = f"curl -X POST 'http://0.0.0.0:8081/models?url={self.model_name}.mar&model_name={self.model_name}&batch_size=128&max_batch_delay=1000&initial_workers=1'"


    def shutdown_local(self, do_rm=False):
        args_str = f'docker stop $(docker ps -a -q --filter "name={self.model_name}")'
        if do_rm:
            args_str = f"docker rm $({args_str})"
        subprocess.run(args_str,
                       shell=True, check=True,
                       executable='/bin/bash')


    def delete_version(self):
        # https://cloud.google.com/ai-platform/prediction/docs/reference/rest/v1/projects.models.versions/delete
        url = f"https://{self.region + '-' if self.region else ''}ml.googleapis.com/v1/projects/{self.project_id}/models/{self.aip_model_name}/versions/{self.model_name}"
        print(f"Deleting: {url}")

        future = self.session.delete(url, timeout=self.timeout, hooks={
                                   "response": response_to_json_hook}, headers=self.auth_headers)

        resp = future.result()
        if resp.status_code == 200:
            print("Success")
        else:
            print(f"\nFailed:\n{url}\n{resp}\n{resp.data}")


    def is_ready(self) -> bool:
        """ Deployed version ready? Raises if Version not deployed"""
        # https://cloud.google.com/ai-platform/prediction/docs/reference/rest/v1/projects.models/get
        url = f"https://{self.region + '-' if self.region else ''}ml.googleapis.com/v1/projects/{self.project_id}/models/{self.aip_model_name}"

        future = self.session.get(url, timeout=self.timeout, hooks={
                                   "response": response_to_json_hook}, headers=self.auth_headers)
        resp = future.result()
        resp.raise_for_status()
        if not "defaultVersion" in resp.data.keys():
            return False
            # raise RuntimeError("No defaultVersion deployed")
        return resp.data["defaultVersion"]["state"] == "READY"
        print(resp.data)
        # raise resp.

    def predict(self, instances, signature_name="serving_default", print_elapsed_time=False, local_run=False):
        '''
        helper function for getting predictions with model name and model endpoint.
        Send json data to a deployed model for prediction.

        Example:
            [{"b64": img_base64_str}]

        Args:
                instances ([Mapping[str: Any]]): Keys should be the names of Tensors
                    your deployed model expects as inputs. Values should be datatypes
                    convertible to Tensors, or (potentially nested) lists of datatypes
                    convertible to tensors.
        :return:
        Mapping[str: any]: dictionary of prediction results defined by the
                    model.
        '''
        time_start = time.time()
        if local_run:
            response = requests.post(f"http://0.0.0.0:8080/predictions/{self.model_name}",json={'signature_name': signature_name, 'instances': instances})
            response.data = response.json()
            response.data["predictions"] = self.output_parser.to_list(response.data['predictions'])
        else:
            future = self.get_inference_future(instances=instances)
            response = future.result()
        print("RESPONSE", response, self.aip_inference_url)
        if print_elapsed_time:
            print("Elapsed time until model version is ready: %.2f minutes" % ((time.time() - time_start) / 60 ))

        if 'error' in response.data:
            raise RuntimeError(response['error'])

        return response.data['predictions']

    def get_inference_future(self, instances, signature_name="serving_default", timeout=60, local_run=False):
        """ Send future sessions to aip deployed model. 

        :param instances: List of instances which should be sended to AIP. e.g. [{"b64": base64_str}]
        :return: Future request which sends response asynchronously
        :rtype: Future 
        """

        # set timeout for request
        socket.setdefaulttimeout(timeout)
        body = {'signature_name': signature_name, 'instances': instances}
        if local_run:
            return self.session.post(f"http://0.0.0.0:8080/predictions/{self.model_name}",json=body, hooks={"response": response_to_json_hook}, timeout=timeout)
        else:
            return self.session.post(self.aip_inference_url, json=body, timeout=timeout, hooks={"response": response_to_json_hook}, headers=self.auth_headers)

    def get_future_result(self, future, instances, max_retry_requests=6, timeout=60, local_run=False):
        retry_counter = 0
        while retry_counter < max_retry_requests:
            time.sleep(retry_counter * 2)
            try:
                resp = future.result()
                return self.output_parser.to_list(resp.data['predictions'])
            except Exception as e:

                # We land here if

                # Response status code: 503 and type: <class 'dict'> and data {'code': 503, 'type': 'ServiceUnavailableException', 'message': 'Model "pytorch_model" Version null" has no worker to serve inference request.
                # Please use scale workers API to add workers.'} [while running 'Preprocess + Append Feature Vector 2']

                # Response status code: 404 and type: <class 'dict'> and data {'error': {'code': 404, 'message': 'Requested entity was not found.', 'status': 'NOT_FOUND'}} [while running 'Preprocess + Append Feature Vector 2']
                self.reload_auth_headers()
            # retry and get new future
            future = self.get_inference_future(instances, local_run=local_run, timeout=timeout)
            retry_counter += 1

        resp = future.result()
        raise ValueError(
            f"Could not receive valid AIP predictions after {max_retry_requests} retries. Response status code: {resp.status_code} and type: {type(resp.data)} and data {resp.data}")


def get_aip_inference_url(aip_model_endpoint, aip_model_str):
    return f"{aip_model_endpoint}/v1/{aip_model_str}:predict"
