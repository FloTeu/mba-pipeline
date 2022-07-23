"""
    Functions and classes to allow async call of sagemaker models
"""
import aiohttp

from typing import Union, Optional
from aiohttp_retry import RetryClient, RetryOptionsBase
from botocore.awsrequest import AWSPreparedRequest
from sagemaker import Session



class MWSagemakerSession(Session):
    # Custom async class to enable using aiotthp requests to AWS Sagemaker

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aiohttp_session: Union[RetryClient, aiohttp.ClientSession] = None

    def is_session_closed(self):
        return self.aiohttp_session.closed if type(self.aiohttp_session) == aiohttp.ClientSession else self.aiohttp_session._closed

    async def open_aiohttp_session(self, retry_options: Optional[RetryOptionsBase]=None, timeout=None, connector: Optional[aiohttp.TCPConnector]=None):
        timeout = aiohttp.ClientTimeout(total=timeout) if timeout else aiohttp.helpers.sentinel
        # Make to retry client if retry options are provided but current session is normal aiohttp session
        if self.aiohttp_session == None or self.is_session_closed() or (retry_options and type(self.aiohttp_session) == aiohttp.ClientSession):
            # Either init a retry session client or normal aiohttp session
            if retry_options:
                self.aiohttp_session = RetryClient(retry_options=retry_options, timeout=timeout)
            else:
                self.aiohttp_session = aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def close_aiohttp_session(self):
        if self.aiohttp_session != None:
            await self.aiohttp_session.close()

    def request_args2aws_prepared_request(self, request_args) -> AWSPreparedRequest:
        # Following code is copied by botocore/client.py and botocore/endpoint.py
        operation_name='InvokeEndpoint'
        operation_model = self.sagemaker_runtime_client._service_model.operation_model(operation_name)

        request_context = {
            'client_region': self.sagemaker_runtime_client.meta.region_name,
            'client_config': self.sagemaker_runtime_client.meta.config,
            'has_streaming_input': operation_model.has_streaming_input,
            'auth_type': operation_model.auth_type,
        }
        request_dict = self.sagemaker_runtime_client._convert_to_request_dict(
            request_args, operation_model, context=request_context)

        return self.sagemaker_runtime_client._endpoint.create_request(request_dict, operation_model)

    @staticmethod
    def bytes2strings(request: AWSPreparedRequest) -> AWSPreparedRequest:
        """ Function to convert bytes in request to strings (AWS allows only string headers for example)
        """
        for key, value in request.headers._dict.items():
            if type(value) == bytes:
                request.headers._dict[key] = value.decode()

    async def ainvoke_endpoint(
            self,
            retry_options: Optional[RetryOptionsBase]=None,
            timeout: Optional[float] = None,
            **request_args
    ):
        """Invoke the endpoint.

            Note: Custome change of original invoke_endpoint() function.
            Request is sended with aiohttp rather than urllib3 lib

        """
        await self.open_aiohttp_session(retry_options=retry_options, timeout=timeout)
        request = self.request_args2aws_prepared_request(request_args)
        self.bytes2strings(request)

        # send async request and retry if retry_options are provided
        sagemaker_response = await self.aiohttp_session.post(request.url, data=request.body, headers=request.headers)
        #sagemaker_response = await self.aiohttp_session.post(request.url, data=request.body, headers=request.headers, raise_for_status=True)
        json_data = await sagemaker_response.json()

        # TODO: How to close aiohttp session again?
        return {"BodyDeserialized": json_data, "ContentType": request_args["ContentType"]}
