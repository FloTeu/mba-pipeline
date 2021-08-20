import os
import time
import subprocess
import socket

from google.cloud import storage
from googleapiclient import discovery
from requests_futures.sessions import FuturesSession

import mwfunctions.constants.ml_constants as mlc
from mwfunctions.cloud.auth import get_service_account_id_token, get_id_token_header
from mwfunctions.cloud.storage import read_json_as_dictg


def response_to_json_hook(resp, *args, **kwargs):
    # parse the json storing the result on the response object
    resp.data = resp.json()


class AI_Model():
    def __init__(self, model_gs_url, region=None, project_id='mba-pipeline', future_sessions_max_workers=2):
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
        
        self.meta_tags_dict = read_json_as_dictg(f"{model_gs_url}/model_meta.json")["tags"]

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
        self.overwrite_auth_headers()

        self.session = FuturesSession(
            max_workers=self.future_sessions_max_workers)  # 8 is default

        # POST https://{endpoint}/v1/{parent=projects/*}/models
        self.aip_projects_models_create_url = f"https://{self.region + '-' if self.region else ''}ml.googleapis.com/v1/projects/{self.project_id}/models"

        self.aip_inference_url = "{0}/v1/{1}:predict".format(self.meta_tags_dict.get("aip_model_endpoint", ""), self.meta_tags_dict.get("aip_model_str", ""))

    def overwrite_auth_headers(self):
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
        # In pytorch for batch inference we currently cannot deploy with automatically inferring
        # the min and max workers. Therefor we have to provide them
        # ATM we do not use this
        # assert num_worker, "For pytorch u have to provide valid num_worker"
        # If we use pytorch we have to build a custom container
        # As we need mvfunctions we build the container with cloudbuild.yaml under mvtools
        # We use a subprocess with gcloud command for this
        # Example command
        # gcloud builds submit . --config=pytorch_cloudbuild.yaml --substitutions=_PROJECT_ID="image-analysis-253512-dev",_CLOUD_STORAGE_PATH="gs://wslkjsdfg-mlflow-dsfgkbjesrih/8/d076677606ab4ab79f373d2071914dea/artifacts/exporter/arcface_512x512_nfnet_l0_mish.mar"
        # gcloud builds submit /home/r_beckmann/projects/mvtools/mvtools/cloud/ai_engine/ --config=/home/r_beckmann/projects/mvtools/mvtools/cloud/ai_engine/pytorch_cloudbuild.yaml --substitutions=_PROJECT_ID=image-analysis-253512-dev,_CLOUD_STORAGE_PATH=gs://wslkjsdfg-mlflow-dsfgkbjesrih/8/d076677606ab4ab79f373d2071914dea/artifacts/exporter/arcface_512x512_nfnet_l0_mish.mar,_AI_MODEL_NAME=ML_MODEL_d076677606ab4ab79f373d2071914dea,_REGION=europe-west-1
        source_dir = os.path.dirname(os.path.realpath(__file__))
        # Mar file is model archive file
        gcs_mar_file = f"{self.model_gs_url}/exporter/{self.model_name}.mar"
        build_yaml = f"{source_dir}/pytorch_cloudbuild.yaml"
        assert os.path.exists(build_yaml), "Could not find cloud_build yaml"

        if not build_local:
            # no-cache not working with config
            args = ["gcloud", "builds", "submit", source_dir,
                    f"--config={build_yaml}",
                    f'--substitutions=_PROJECT_ID=merchwatch-dev,'
                    f'_CLOUD_STORAGE_PATH={gcs_mar_file},'
                    f'_AIP_MODEL_NAME={self.aip_model_name},'
                    f'_NUM_WORKER={num_worker},'
                    f'_MODEL_NAME={self.model_name},'
                    f'_REGION={self.region}']
        else:
            args = ["cloud-build-local", 
                    f"--config={build_yaml}",
                    f"--dryrun=False",
                    f'--substitutions=_PROJECT_ID=merchwatch-dev,'
                    f'_CLOUD_STORAGE_PATH={gcs_mar_file},'
                    f'_AIP_MODEL_NAME={self.aip_model_name},'
                    f'_NUM_WORKER={num_worker},'
                    f'_MODEL_NAME={self.model_name},'
                    f'_REGION={self.region}',
                    source_dir]
        subprocess.run(args, check=True)

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
            self.mvflow.set_aip_model(self.endpoint, self.aip_model_str)
            if wait_until_finished:
                while not self.is_ready():
                    time.sleep(60)
        print(resp)
        print(resp.data)

    def delete_version(self):
        # https://cloud.google.com/ai-platform/prediction/docs/reference/rest/v1/projects.models.versions/delete
        url = f"https://{self.region + '-' if self.region else ''}ml.googleapis.com/v1/projects/{self.project_id}/models/{self.aip_model_name}/versions/{self.model_name}"
        print(f"Deleting: {url}")

        future = self.session.delete(url, timeout=self.timeout, hooks={
                                   "response": response_to_json_hook}, headers=self.auth_headers)

        resp = future.result()
        if resp.status_code == 200:
            # delete ai-engine tags from run
            self.mvflow.remove_aip_model()
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
        
        

    def predict(self, instances, signature_name="serving_default"):
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

        future = self.get_inference_future(instances=instances)
        resp = future.result()

        if 'error' in response:
            raise RuntimeError(response['error'])

        return response['predictions']

    def get_inference_future(self, instances, signature_name="serving_default", timeout=60):
        """ Send future sessions to aip deployed model. 

        :param instances: List of instances which should be sended to AIP. e.g. [{"b64": base64_str}]
        :return: Future request which sends response asynchronously
        :rtype: Future 
        """

        # set timeout for request
        socket.setdefaulttimeout(timeout)
        body = {'signature_name': signature_name, 'instances': instances}
        return self.session.post(self.aip_inference_url, json=body, timeout=timeout, hooks={"response": response_to_json_hook}, headers=self.auth_headers)

    def get_future_results(self, future, img_b64_str, max_retry_requests=6):
        from mvfunctions.cloud.auth import get_service_account_id_token, get_id_token_header
        retry_counter = 0
        while retry_counter < max_retry_requests:
            try:
                resp = future.result()
                return self.output_parser.to_list(resp.data['predictions'])
            except Exception as e:

                # We land here if

                # Response status code: 503 and type: <class 'dict'> and data {'code': 503, 'type': 'ServiceUnavailableException', 'message': 'Model "pytorch_model" Version null" has no worker to serve inference request.
                # Please use scale workers API to add workers.'} [while running 'Preprocess + Append Feature Vector 2']

                # Response status code: 404 and type: <class 'dict'> and data {'error': {'code': 404, 'message': 'Requested entity was not found.', 'status': 'NOT_FOUND'}} [while running 'Preprocess + Append Feature Vector 2']
                self.overwrite_auth_header()
            # retry and get new future
            future = self.aip_async_inference(img_b64_str)
            retry_counter += 1

        resp = future.result()
        raise ValueError(
            f"Could not receive valid AIP predictions after {max_retry_requests} retries. Response status code: {resp.status_code} and type: {type(resp.data)} and data {resp.data}")


def get_aip_inference_url(aip_model_endpoint, aip_model_str):
    return f"{aip_model_endpoint}/v1/{aip_model_str}:predict"


def aip_inference_future(session: FuturesSession, instances, headers, url=None, aip_model_endpoint=None, aip_model_str=None, signature_name="serving_default", timeout=60):
    """ Send future sessions to aip deployed model. 
        Note:
            headers: Must contain at least contain Authorization e.g. {"Authorization": f"Bearer {get_service_account_id_token()}"}
            url: Can be created with get_aip_inference_url()

    :param session: Session object of type FuturesSession
    :type session: FuturesSession
    :param instances: List of instances which should be sended to AIP. e.g. [{"b64": base64_str}]
    :param headers: Dict of headers which are used by requests library in background
    :type headers: dict
    :return: Future request which sends response asynchronously
    :rtype: Future 
    """

    import socket

    assert url or (
        aip_model_endpoint and aip_model_str), "Either url or at least both, aip_model_endpoint and aip_model_str, must be provided."

    # set timeout for request
    socket.setdefaulttimeout(timeout)

    if not url:
        url = get_aip_inference_url(aip_model_endpoint, aip_model_str)
    body = {'signature_name': signature_name, 'instances': instances}
    future = session.post(url, json=body, timeout=timeout, hooks={
                          "response": response_to_json_hook}, headers=headers)
    return future


def submit_training(jobId, trainingInput,
                    project_id='mba-pipeline'):
    '''

    :param jobId:
    :param training_inputs:
    example:
        training_inputs = {'scaleTier': 'BASIC',
            'packageUris': ['gs://mv_model_deployments/marketvisionfunctions/mvfunctions-0.1.tar.gz',
                            'gs://mv_model_deployments/vectorindex/vectorindex-0.1.tar.gz'],
            'pythonModule': 'trainer.train_index_id_features',
            'args': ['--json_gs_url_bq_table_id','image-analysis-253512:fashion_eu.peterhahn_images_mvdelf_test123_features',
                    '--mlflow_run_url','http://35.198.118.119/#/experiments/0/runs/b07bfec40f204dbab228350f9d86cada/',
                    '--local_output_root_dir', '/tmp',
                    '--match_oneself', 'True'],
            'region': 'europe-west3',
            'jobDir': 'gs://staging.image-analysis-253512.appspot.com',
            'runtimeVersion': '2.3',
            'pythonVersion': '3.7'}

    :param project_id:
    :return:
    '''

    job_spec = {'jobId': jobId, 'trainingInput': trainingInput}
    project_id = 'projects/{}'.format(project_id)

    #  "Field: training_input Error: Training is not supported on this endpoint."
    # -> keine regional endpoints possible
    cloudml = discovery.build('ml', 'v1')

    request = cloudml.projects().jobs().create(body=job_spec, parent=project_id)
    response = request.execute()

    print(response)
    return response
