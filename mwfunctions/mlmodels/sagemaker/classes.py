import urllib3

from sagemaker.huggingface import HuggingFacePredictor, HuggingFaceModel
from sagemaker.session import production_variant, Session
from sagemaker.local.utils import get_docker_host
from sagemaker.utils import get_config_value, _module_import_error

from mwfunctions.io.async_io_fns import create_async_http_session, post_url_response


class AsyncSagemakerSession(Session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        t = 0
        # TODO include a async session which implements invoke_endpoint async
        # self.boto_session.client(
        #     "runtime.sagemaker", config=config
        # )
        # TODO: customize session with config params like max connections timeout etc.
        self.aiohttp_session = create_async_http_session()
        # Replace sync function with custom async function
        #self.sagemaker_runtime_client.invoke_endpoint = self.ainvoke_endpoint
        # TODO Is it realy always 8080? Normaly port is fetched out of LocalSagemakerRuntimeClient config
        self.serving_port = 8080

        # TODO: Remove this again
        self.http = urllib3.PoolManager()



    async def ainvoke_endpoint(
            self,
            Body,
            EndpointName,  # pylint: disable=unused-argument
            ContentType=None,
            Accept=None,
            CustomAttributes=None,
            TargetModel=None,
            TargetVariant=None,
            InferenceId=None,
    ):
        """Invoke the endpoint.

            Note: Custome change of original invoke_endpoint() function.
            Request is sended with aiohttp rather than urllib3 lib

        Args:
            Body: Input data for which you want the model to provide inference.
            EndpointName: The name of the endpoint that you specified when you
                created the endpoint using the CreateEndpoint API.
            ContentType: The MIME type of the input data in the request body (Default value = None)
            Accept: The desired MIME type of the inference in the response (Default value = None)
            CustomAttributes: Provides additional information about a request for an inference
                submitted to a model hosted at an Amazon SageMaker endpoint (Default value = None)
            TargetModel: The model to request for inference when invoking a multi-model endpoint
                (Default value = None)
            TargetVariant: Specify the production variant to send the inference request to when
                invoking an endpoint that is running two or more variants (Default value = None)
            InferenceId: If you provide a value, it is added to the captured data when you enable
               data capture on the endpoint (Default value = None)

        Returns:
            object: Inference for the given input.
        """

        url = "http://%s:%d/invocations" % (get_docker_host(), self.serving_port)
        headers = {}

        if ContentType is not None:
            headers["Content-type"] = ContentType

        if Accept is not None:
            headers["Accept"] = Accept

        if CustomAttributes is not None:
            headers["X-Amzn-SageMaker-Custom-Attributes"] = CustomAttributes

        if TargetModel is not None:
            headers["X-Amzn-SageMaker-Target-Model"] = TargetModel

        if TargetVariant is not None:
            headers["X-Amzn-SageMaker-Target-Variant"] = TargetVariant

        if InferenceId is not None:
            headers["X-Amzn-SageMaker-Inference-Id"] = InferenceId

        # The http client encodes all strings using latin-1, which is not what we want.
        if isinstance(Body, str):
            Body = Body.encode("utf-8")
        r = self.http.request("POST", url, body=Body, preload_content=False, headers=headers)
        r2 = await post_url_response(url, data=Body, headers=headers, session=self.aiohttp_session)

        return {"Body": r, "ContentType": Accept}

class AsyncHuggingFacePredictor(HuggingFacePredictor):

    def __init__(
        self,
        endpoint_name,
        sagemaker_session=None,
        *args,
        **kwargs,
    ):
        sagemaker_session = sagemaker_session if sagemaker_session else AsyncSagemakerSession()
        super().__init__(endpoint_name, sagemaker_session=sagemaker_session, *args, **kwargs)

    async def apredict(
        self,
        data,
        initial_args=None,
        target_model=None,
        target_variant=None,
        inference_id=None,
    ):
        """Return the inference from the specified endpoint.

        Args:
            data (object): Input data for which you want the model to provide
                inference. If a serializer was specified when creating the
                Predictor, the result of the serializer is sent as input
                data. Otherwise the data must be sequence of bytes, and the
                predict method then sends the bytes in the request body as is.
            initial_args (dict[str,str]): Optional. Default arguments for boto3
                ``invoke_endpoint`` call. Default is None (no default
                arguments).
            target_model (str): S3 model artifact path to run an inference request on,
                in case of a multi model endpoint. Does not apply to endpoints hosting
                single model (Default: None)
            target_variant (str): The name of the production variant to run an inference
                request on (Default: None). Note that the ProductionVariant identifies the
                model you want to host and the resources you want to deploy for hosting it.
            inference_id (str): If you provide a value, it is added to the captured data
                when you enable data capture on the endpoint (Default: None).

        Returns:
            object: Inference for the given input. If a deserializer was specified when creating
                the Predictor, the result of the deserializer is
                returned. Otherwise the response returns the sequence of bytes
                as is.
        """

        request_args = self._create_request_args(
            data, initial_args, target_model, target_variant, inference_id
        )
        # TODO: Call this function async
        response = await self.sagemaker_session.ainvoke_endpoint(**request_args)
        self.sagemaker_session.sagemaker_runtime_client.
        return self._handle_response(response)