from fastapi import Security, HTTPException
from starlette.status import HTTP_403_FORBIDDEN
from fastapi.security.api_key import APIKeyQuery, APIKeyHeader
from mwfunctions.environment import is_debug
# Turorial for api key and fastapi (https://fastapi.tiangolo.com/tutorial/query-params/)


API_KEY_NAME = "access_token"



def check_api_key(api_key, api_key_name, allow_unauth_debug=False):
        API_KEY_NAME = "access_token"
   

        # API key can be either in query param or header, but at least in one
        api_key_query = APIKeyQuery(name=api_key_name, auto_error=False)
        api_key_header = APIKeyHeader(name=api_key_name, auto_error=False)

        async def get_api_key(
                api_key_query: str = Security(api_key_query),
                api_key_header: str = Security(api_key_header),
                ):

                if allow_unauth_debug and is_debug():
                        return api_key_header
                elif api_key_query == api_key:
                        return api_key_query
                elif api_key_header == api_key:
                        return api_key_header
                else:
                        raise HTTPException(
                        status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
                        )
                
        return get_api_key


def get_allow_all_CORSMiddleware_params_dict():
    return {"allow_origins": ["*"],
            "allow_credentials": True,
            "allow_methods": ["*"],
            "allow_headers": ["*"]}