import os
import logging
from re import I

_mv_env_vars = [
    "DEBUG",
    "LOG_LEVEL",
    "CLOUD_LOGGING"
]

def booleanize(s):
    return s.lower() in ['true', '1']

def set_cloud_logging():
    os.environ["CLOUD_LOGGING"] = "True"

def cloud_logging():
    return booleanize(os.environ["CLOUD_LOGGING"])

def is_debug():
    return booleanize(os.getenv("DEBUG", "False"))

def get_log_level():
    log_level_name = os.environ["LOG_LEVEL"]
    log_level = logging.getLevelName(log_level_name)
    return log_level

def get_gcp_project():
    """ Google Cloud Platform Project ;) """
    try:
        try:
            project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
        except:
            project_id = os.environ["GCP_PROJECT"]
    except:
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS", None):
            raise KeyError(f'While using service account {os.environ["GOOGLE_APPLICATION_CREDENTIALS"]} you have to set either GOOGLE_CLOUD_PROJECT or GCP_PROJECT')
        raise KeyError("GOOGLE_CLOUD_PROJECT or GCP_PROJECT not set")
    return project_id

def get_gcp_credentials():
    try:
        credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    except:
        import google.auth
        # Should raise exception of not set
        credentials, _ = google.auth.default()
    return credentials


# Mit assert_gcp_auth setzen. Dann kann man im Terminal auch über export GOOGLE_APPLICATION_CREDENTIALS=... setzen
def overwrite_gcp_credentials(credentials_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

def overwrite_gcp_project(project_id):
    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

def assert_gcp_auth(credentials=None, project_id=None):
    """
    Check or set GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT. Already set env variables have priority.

    Not sure what happens if you use default credentials on a different effective project. Effective project
    is the activated project on the default account.

    :param credentials: Credentials filepath
    :param project_id: Full project_id
    :return:
    """
    _project=None

    # GOOGLE_APPLICATION_CREDENTIALS
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'].endswith(".json")
    except KeyError:
        if credentials:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        else:
            # LOGGER.warning("GOOGLE_APPLICATION_CREDENTIALS not set, try inferring auth default")
            import google.auth
            # Should raise exception of not set
            credentials, _project = google.auth.default()
            # LOGGER.info(f"Using default credentials (effective project: {_project})")
            # return credentials, project_id

    # If we are here, we have at least default credentials

    # GOOGLE_CLOUD_PROJECT
    try:
        os.environ['GOOGLE_CLOUD_PROJECT'] = get_gcp_project()
    except KeyError:
        if project_id:
            os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
        elif _project:  # From default auth
            os.environ['GOOGLE_CLOUD_PROJECT'] = _project

    # Assert env is set
    # os.environ['GOOGLE_CLOUD_PROJECT']



def get_cpu_count():
    import multiprocessing
    return multiprocessing.cpu_count()