import google.auth.transport.requests
import google.oauth2.id_token
import google.auth
import google.auth.transport.requests

def get_oauth2_id_token_by_url(service_url):
    """Gets a service url and returns a oauth2 id token

    :param service_url: Url of google cloud service like cloud run, cloud function etc.
    :type service_url: str
    """
    auth_req = google.auth.transport.requests.Request()
    return google.oauth2.id_token.fetch_id_token(auth_req, service_url)


def get_service_account_id_token(scopes=None):
    """
        scopes:
            * List of scope urls e.g. ["https://www.googleapis.com/auth/cloud-platform"] for access to AI platform
            * Scopes define on which services a request should get access
            * Find scope urls her: https://developers.google.com/identity/protocols/oauth2/scopes
    """
    credentials, project = google.auth.default(scopes=scopes)
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token


def get_id_token_header(id_token):
    return f"Bearer {id_token}"
