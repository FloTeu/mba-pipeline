import logging
from google.oauth2 import service_account

from mwfunctions import environment

def has_cloud_log_write_permission():
    from googleapiclient import discovery

    """Tests IAM permissions of the caller"""

    credentials = environment.get_gcp_credentials()
    if type(credentials) == str:
        credentials = service_account.Credentials.from_service_account_file(
            filename=credentials,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    service = discovery.build(
        "cloudresourcemanager", "v1", credentials=credentials
        , cache_discovery=False)

    permissions = {
        "permissions": [
            "logging.logEntries.create"
        ]
    }

    request = service.projects().testIamPermissions(
        resource=environment.get_gcp_project(), body=permissions
    )
    returnedPermissions = request.execute()
    return type(returnedPermissions) == dict and "permissions" in returnedPermissions and ["logging.logEntries.create"] == returnedPermissions["permissions"]


def get_logger(name, log_level=logging.INFO, do_cloud_logging=False, labels_dict=None, excluded_loggers=('tensorflow',)):
    """
    Setup a logger. Environment variables have priority.

    Environment variables:
        LOG_LEVEL
        DO_CLOUD_LOGGING

    :param labels_dict: Für google_cloud_logging, wird unter "labels" in der logging json angezeigt.
                        Danach kannst du suchen bei google cloud logging wenn du "labels.<key> = <value>" einsetzt.

    :return: logger
    """
    import os
    from contextlib import suppress
    from mwfunctions import environment

    with suppress(KeyError):
        log_level = environment.get_log_level()

    logging.basicConfig(level=log_level)

    with suppress(KeyError):
        do_cloud_logging = environment.cloud_logging()

    if do_cloud_logging and has_cloud_log_write_permission():
        return get_googled_logger(name, excluded_loggers=excluded_loggers, log_level=log_level, labels_dict=labels_dict)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        return logger

def get_googled_logger(name, labels_dict, excluded_loggers=('tensorflow',), log_level=logging.WARN):
    """ Setup a google logger with labels.
    In gcloud logging labels get saved as entry in the logging json. You can search by:
    labels.<key> = <value> for the entry.
    """
    import google.cloud.logging
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client, name=name, labels=labels_dict)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    setup_logging(handler, excluded_loggers=excluded_loggers)
    return logger
