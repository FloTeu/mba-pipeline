# import urllib3
# import json
#
# from sagemaker.huggingface import HuggingFacePredictor, HuggingFaceModel
# from sagemaker.session import production_variant, Session
# from sagemaker.local.utils import get_docker_host
# from sagemaker.utils import get_config_value, _module_import_error
# from botocore.awsrequest import AWSPreparedRequest
# from botocore.response import StreamingBody
#
# from mwfunctions.io.async_io_fns import acreate_async_http_session, post_url_response, acreate_async_retry_client, send_and_retry_http, RequestMethod
# 
#
# class AsyncSagemakerSession(Session):
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         t = 0
#         # TODO include a async session which implements invoke_endpoint async
#         # self.boto_session.client(
#         #     "runtime.sagemaker", config=config
#         # )
#         self.aiohttp_session = None
#
#     async def open_aiohttp_session(self):
#         # TODO: customize session with config params like max connections timeout etc.
#         if self.aiohttp_session == None:
#             self.aiohttp_session = await acreate_async_http_session()
#
#     def request_args2aws_prepared_request(self, request_args) -> AWSPreparedRequest:
#         # Following code is copied by botocore/client.py and botocore/endpoint.py
#         operation_name='InvokeEndpoint'
#         operation_model = self.sagemaker_runtime_client._service_model.operation_model(operation_name)
#
#         request_context = {
#             'client_region': self.sagemaker_runtime_client.meta.region_name,
#             'client_config': self.sagemaker_runtime_client.meta.config,
#             'has_streaming_input': operation_model.has_streaming_input,
#             'auth_type': operation_model.auth_type,
#         }
#         request_dict = self.sagemaker_runtime_client._convert_to_request_dict(
#             request_args, operation_model, context=request_context)
#
#         return self.sagemaker_runtime_client._endpoint.create_request(request_dict, operation_model)
#
#     @staticmethod
#     def bytes2strings(request: AWSPreparedRequest) -> AWSPreparedRequest:
#         for key, value in request.headers._dict.items():
#             if type(value) == bytes:
#                 request.headers._dict[key] = value.decode()
#
#         # if type(request.body) == bytes:
#         #     request.body = request.body.decode()
#
#
#     async def ainvoke_endpoint(
#             self,
#             retries=5,
#             init_wait_time=0.5,
#             backoff=3,
#             http_status_codes_to_retry=[400, 500, 502, 503, 504],
#             **request_args
#     ):
#         """Invoke the endpoint.
#
#             Note: Custome change of original invoke_endpoint() function.
#             Request is sended with aiohttp rather than urllib3 lib
#
#         """
#         await self.open_aiohttp_session()
#         request = self.request_args2aws_prepared_request(request_args)
#         # t = requests.post(request.url, data=request.body, headers=request.headers)
#         self.bytes2strings(request)
#         #sagemaker_response = await post_url_response(request.url, data=request.body, headers=request.headers, session=self.aiohttp_session)
#         try:
#             json_data, sagemaker_response = await send_and_retry_http(request.url, RequestMethod.POST,
#                                                            retries=retries,
#                                                            init_wait_time=init_wait_time, # half a second
#                                                            backoff=backoff,
#                                                            http_status_codes_to_retry=http_status_codes_to_retry,
#                                                            data=request.body,
#                                                            headers=request.headers,
#                                                            session=self.aiohttp_session)
#             return {"Response": sagemaker_response, "JsonData": json_data, "ContentType": request_args["ContentType"]}
#         except Exception as e:
#             return {"Response": None, "JsonData": None, "ContentType": request_args["ContentType"]}
#
#
# class AsyncHuggingFacePredictor(HuggingFacePredictor):
#
#     def __init__(
#         self,
#         endpoint_name,
#         sagemaker_session=None,
#         *args,
#         **kwargs,
#     ):
#         sagemaker_session = sagemaker_session if sagemaker_session else AsyncSagemakerSession()
#         super().__init__(endpoint_name, sagemaker_session=sagemaker_session, *args, **kwargs)
#
#     async def apredict(
#         self,
#         data,
#         initial_args=None,
#         target_model=None,
#         target_variant=None,
#         inference_id=None,
#         **retry_kwargs
#     ):
#         """Return the inference from the specified endpoint.
#
#         Args:
#             data (object): Input data for which you want the model to provide
#                 inference. If a serializer was specified when creating the
#                 Predictor, the result of the serializer is sent as input
#                 data. Otherwise the data must be sequence of bytes, and the
#                 predict method then sends the bytes in the request body as is.
#             initial_args (dict[str,str]): Optional. Default arguments for boto3
#                 ``invoke_endpoint`` call. Default is None (no default
#                 arguments).
#             target_model (str): S3 model artifact path to run an inference request on,
#                 in case of a multi model endpoint. Does not apply to endpoints hosting
#                 single model (Default: None)
#             target_variant (str): The name of the production variant to run an inference
#                 request on (Default: None). Note that the ProductionVariant identifies the
#                 model you want to host and the resources you want to deploy for hosting it.
#             inference_id (str): If you provide a value, it is added to the captured data
#                 when you enable data capture on the endpoint (Default: None).
#
#         Returns:
#             object: Inference for the given input. If a deserializer was specified when creating
#                 the Predictor, the result of the deserializer is
#                 returned. Otherwise the response returns the sequence of bytes
#                 as is.
#         """
#
#         request_args = self._create_request_args(
#             data, initial_args, target_model, target_variant, inference_id
#         )
#         # TODO: Call this function async
#         response_dict = await self.sagemaker_session.ainvoke_endpoint(**{**retry_kwargs, **request_args})
#         #response = await self.sagemaker_session.sagemaker_runtime_client.invoke_endpoint_async(**request_args)
#         #response = self.sagemaker_session.sagemaker_runtime_client.invoke_endpoint(**request_args)
#
#         return self._handle_async_response(response_dict)
#
#     def _handle_async_response(self, response_dict):
#         #response = response_dict["Response"]
#         content_type = response_dict.get("ContentType", "application/json")
#         if content_type == "application/json":
#             return response_dict["JsonData"]
#         else:
#             raise NotImplementedError
