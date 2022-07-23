import asyncio

from typing import Any
from abc import ABC, abstractmethod

from mwfunctions.cloud.sagemaker.async_fns import MWSagemakerSession
from sagemaker.deserializers import JSONDeserializer
from sagemaker.serializers import JSONSerializer
from sagemaker.predictor import Predictor


class MWPredictorAbstract(ABC):

    @abstractmethod
    def predict(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    async def apredict(self, *args, **kwargs) -> Any:
        # asynchronously send predict request
        pass

    @abstractmethod
    async def is_ready(self) -> bool:
        # Whether predictor can be called (cloud instance is ready/deployed)
        pass


class MWSagemakerPredictor(Predictor, MWPredictorAbstract):

    def __init__(
        self,
        endpoint_name,
        sagemaker_session: MWSagemakerSession=None,
        serializer=JSONSerializer(),
        deserializer=JSONDeserializer(),
    ):
        """Initialize an ``HuggingFacePredictor``.

        Args:
            endpoint_name (str): The name of the endpoint to perform inference
                on.
            sagemaker_session (sagemaker.session.Session): Session object that
                manages interactions with Amazon SageMaker APIs and any other
                AWS services needed. If not specified, the estimator creates one
                using the default AWS configuration chain.
            serializer (sagemaker.serializers.BaseSerializer): Optional. Default
                serializes input data to .npy format. Handles lists and numpy
                arrays.
            deserializer (sagemaker.deserializers.BaseDeserializer): Optional.
                Default parses the response from .npy format to numpy array.
        """
        sagemaker_session = sagemaker_session if sagemaker_session else MWSagemakerSession()
        super(MWSagemakerPredictor, self).__init__(
            endpoint_name,
            sagemaker_session,
            serializer=serializer,
            deserializer=deserializer,
        )

    async def apredict(
        self,
        data,
        initial_args=None,
        target_model=None,
        target_variant=None,
        inference_id=None,
        **retry_kwargs
    ):
        """ Asynchronous function of already existing predict() function of Sagemaker Predictor
        Return the inference from the specified endpoint.

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
        response_dict = await self.sagemaker_session.ainvoke_endpoint(**{**retry_kwargs, **request_args})
        return self._handle_response(response_dict)

    def _handle_response(self, response_dict):
        """ Wrapper class of Sagemaker Predictor, which returns already deserialized Body directly
        :param response_dict:
        :return:
        """
        if "BodyDeserialized" in response_dict:
            return response_dict["BodyDeserialized"]
        else:
            return super()._handle_response(response_dict)

    def is_ready(self):
        """ Whether model is ready for inferencing
            Simple heuristic: Model is ready if the response is faster than 3 seconds
        :return:
        """
        raise NotImplementedError

    async def ais_ready(self):
        """ Whether model is ready for inferencing
            Simple heuristic: Model is ready if the response is faster than 2 seconds
            # TODO: Find better solution
        :return:
        """
        try:
            t = await self.apredict({"inputs": ""}, timeout=2)
            await self.sagemaker_session.close_aiohttp_session()
            return True
        except asyncio.TimeoutError:
            await self.sagemaker_session.close_aiohttp_session()
            return False

    async def await_until_predictor_is_ready(self):
        while True:
            if await self.ais_ready():
                # model is ready so break loop and return
                return None
            else:
                print("Sleep for on second because model is not ready")
                # sleep async for 1 second
                await asyncio.sleep(1)



