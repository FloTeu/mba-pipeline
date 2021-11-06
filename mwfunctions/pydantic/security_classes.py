from mwfunctions.pydantic.base_classes import MWBaseModel
from pydantic import Field
from typing import Optional, List, Dict
from enum import Enum

class Login(MWBaseModel):
    user_id: str = Field(description="User id can be a user name or a email address of login page")
    password: str = Field(description="Password of login page")
    website_domain: Optional[str] = Field(None, description="domain of website without http and www")

class ProxyItem(MWBaseModel):
    url: str = Field(description="url which can be called to send request through proxy service. (Without auhtentification)")
    name: str = Field(description="Unique name of proxy. Can be the city for example")
    location: str = Field(description="location of proxy, e.g. USA, Germany etc.")
    working: bool = Field(False, description="Whether proxy is approved to work well")

class ProxyServiceSettings(MWBaseModel):
    proxy_provider_login: Login = Field(description="Object contains everything for authentification to proxy provider")
    proxy_items: List[ProxyItem]
    default_port: int
    active: bool = Field(False, description="Whether proxy service is active (account has right to use it, payed etc.)")

class ProxyService(str, Enum):
    PERFECT_PRIVACY = "perfect_privacy"

class EndpointServiceName(str, Enum):
    CLOUD_RUN = "cloud_run"
    CLOUD_FUNCTION = "cloud_function"

class EndpointServiceDevOp(str, Enum):
    DEBUG = "debug"
    DEV = "dev"
    PROD = "prod"

class EndpointServiceTrigger(str, Enum):
    HTTP = "http"
    PUB_SUB = "pub_sub"

class EndpointSettings(MWBaseModel):
    service_name: EndpointServiceName
    devop2url: Dict[EndpointServiceDevOp, str] = Field(description="DevOp status to url with which service can be called")
    trigger: EndpointServiceTrigger

class EndpointId(str, Enum):
    CRAWLER_IMAGE_PIPELINE = "crawler_image_pipeline"
    CRAWLER_SCALE_TO_MOON = "crawler_scale_to_moon"

# class ProxySecuritySettingItem(MWBaseModel):
#     proxy_service: ProxyServices
#     proxy_settings: ProxySettings

import json
class MWSecuritySettings(MWBaseModel):
    proxy_services: Optional[Dict[ProxyService, ProxyServiceSettings]] = Field(None)
    endpoints: Optional[Dict[EndpointId, EndpointSettings]] = Field(None)


    def __init__(self, file_path=None, *args, **kwargs):
        if file_path:
            with open(file_path) as json_file:
                data = json.load(json_file)
            super(MWSecuritySettings, self).__init__(**data)
        else:
            super(MWSecuritySettings, self).__init__(*args, **kwargs)
