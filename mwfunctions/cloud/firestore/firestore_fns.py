from datetime import datetime
from google.cloud import firestore
import logging
import google

from mwfunctions.exceptions import log_suppress
from mwfunctions import environment

logging.basicConfig(level="INFO")
LOGGER = logging.getLogger(__name__)


##########################################################################
# Functions

CLIENT_CACHE = None

def create_client(project=None, use_cache=True):
    """ abstract firestore away """
    global CLIENT_CACHE
    if CLIENT_CACHE is None or not use_cache:
        project_id = project if project else environment.get_gcp_project()
        CLIENT_CACHE = firestore.Client(project=project_id)
    return CLIENT_CACHE

# class CachedFirestoreClient(object):
#     client = None
#     def __init__(self, project=None) -> None:
#         if self.client is None:
#             self.client = create_client(project)
        

# 0.028623104095458984s from bab in der firma
def get_document_snapshot_dict(path=None, collection_path=None, document_id=None, client=None,
                               transaction=None) -> dict:
    """ Provide path EXOR ( collection_name and document_id ) """
    return get_document_snapshot(path, collection_path, document_id, client, transaction=transaction).to_dict()


def _split_correct_path(path):
    # No "/" at start for the python api
    path = path[1:] if path[0] == "/" else path
    collection_path, document_id = "/".join(path.split("/")[:-1]), "/".join(path.split("/")[-1:])
    return collection_path, document_id


def write_document_dict(data_dict, path=None, collection_path=None, document_id=None, overwrite_doc=False, client=None,
                        add_timestamp=False, array_union=False):
    """ Creates needed paths. Updates if not overwrite_doc.

    Args:
        data_dict (dict): Dict of properties which are either set or updated to firestore document
        path (str): Path as seen in web interface
        collection_path (str): firestore path to collection. E.g. col/doc/col/.../col
        doc_id (str): document id which is part of collection in collection_path
        overwrite_doc (bool, optional): Whether to set a document (overwrites existent) or update it. Defaults to False.
        client ([type], optional): Firestore client object. Defaults to None.
        add_timestamp (bool, optional): Whether to set timestamp property or not. Defaults to False.
        array_union (bool, optional): Whether to update existent arrays or overwrite it if do_set is False. Defaults to False.
    """
    assert path != (collection_path and document_id), "Provide path or collection_name and document_id"
    if path:
        collection_path, document_id = _split_correct_path(path)
    client = client if client else create_client()
    if add_timestamp: data_dict.update({'timestamp': datetime.now()})

    # get reference of document which can execute update or set command on document
    doc_ref = client.collection(collection_path).document(str(document_id))

    # Set if overwrite_doc or it does not exist
    if overwrite_doc or not doc_ref.get().exists:
        doc_ref.set(data_dict)
    else:
        # if array_union is True, already existent array should be appended with new data but not overwritten
        # https://cloud.google.com/firestore/docs/manage-data/add-data#update_elements_in_an_array
        if array_union:
            for key, value in data_dict.items():
                if type(value) == list:
                    data_dict.update({key: firestore.ArrayUnion(value)})
        doc_ref.update(data_dict)

def delete_document(path):
    doc_ref = get_document_ref(path)
    doc_ref.delete()

def get_document_ref(path=None, collection_path=None, document_id=None, client=None):
    """ Reference of Firstore document

    :param path: Path to document like col/doc/col/doc/.../doc , defaults to None
    :type path: str, optional
    :param collection_path: Path to collection like col or col/doc/col/.../col, defaults to None
    :type collection_path: str, optional
    :param document_id: Firestore ID of document, defaults to None
    :type document_id: str, optional
    :param client: Firestore client, defaults to None
    :type client: Firestore client, optional
    :return: DocumentReference object of Firestore
    :rtype: DocumentReference
    """
    assert path != (collection_path and document_id), "Provide path or collection_name and document_id"
    if path:
        collection_path, document_id = _split_correct_path(path)
    client = client if client else create_client()
    return client.collection(collection_path).document(document_id)


def get_docs_iterator(collection_path, client=None):
    """ Return all documents of given path/collection path as a generator.

    Args:
        collection_path (str): Collection Firestore path
        client (google.cloud.firestore_v1.client.Client): Firestore client

    Returns:
        iterable
    """

    # No "/" at start for the python api
    collection_path = collection_path[1:] if collection_path[0] == "/" else collection_path
    assert (len(collection_path.split(
        "/")) % 2) != 0, "path needs be a collection path with odd number of '/' seperated elements"
    client = client if client else create_client()
    return client.collection(collection_path).stream()


def get_docs_batch(collection_path, limit, order_by=None, start_after=None, direction="asc", client=None):
    """Return a generator object of one batch(size like limit) of documents

    Args:
        collection_path (str): Collection Firestore path
        limit (int): Size of batch
        order_by (str, optional): Firestore property of document which should be used for ordering. Important for start_after to work properly. Defaults to None.
        start_after (str, optional): Value of Firestore property defined in order_by. One document after this value new batch is started. Defaults to None.
        direction (str, optional): Whether to order in ascending or descending order. Defaults to "asc".
        client (google.cloud.firestore_v1.client.Client): Google cloud Firestore client.Defaults to None".

    Returns:
        iterable
    """

    # No "/" at start for the python api
    collection_path = collection_path[1:] if collection_path[0] == "/" else collection_path
    assert (len(collection_path.split(
        "/")) % 2) != 0, "path needs be a collection path with odd number of '/' seperated elements"
    assert bool(start_after) == bool(order_by), "start_after and order_by needs to be set"

    assert direction in ["asc", "desc"], 'direction elem ["asc", "desc"]'
    direction = firestore.Query.ASCENDING if direction == "asc" else firestore.Query.DESCENDING

    client = client if client else create_client()

    collection_ref = client.collection(collection_path)
    if order_by:
        collection_ref = collection_ref.order_by(order_by, direction=direction)
    if start_after:
        collection_ref = collection_ref.start_after({
            order_by: start_after
        })
    return collection_ref.limit(limit).stream()


def get_document_snapshot(path=None, collection_name=None, document_id=None, client=None, transaction=None):
    """ Provide path EXOR ( collection_name and document_id )
    snapshot.to_dict() for data, snapshot.reference for the document reference id
    """
    return get_document_ref(path, collection_name, document_id, client).get(transaction=transaction)


def validate_document_name(document_name):
    """ validates if document_name is suitable for firestore document name """
    if type(document_name) != str:
        raise ValueError("document_name is not a string")
    else:
        if "." in document_name or ".." in document_name or "/" in document_name:
            raise ValueError("document_name does not fit the firestore naming rules")


def get_field(doc_path, field):
    doc_dict = get_document_snapshot_dict(path=doc_path)
    return doc_dict[field]

def set_field(doc_path, field, field_value):
    write_document_dict({field: field_value},path=doc_path, overwrite_doc=False)

def del_field(doc_path, field):
    with log_suppress(f"Could not find {doc_path} : {field}", google.api_core.exceptions.NotFound):
        doc_ref = get_document_ref(doc_path)
        doc_ref.update({field: firestore.DELETE_FIELD})


def increment_field(doc_path, field, amount):
    doc_ref = get_document_ref(doc_path)
    doc_ref.update({field: firestore.Increment(amount)})


@firestore.transactional
def atomic_add(transaction, doc_path, field, amount, client):
    """ Its only allowed to write once per second... """
    doc_dict = get_document_snapshot_dict(path=doc_path, client=client, transaction=transaction)
    doc_dict[field] += amount
    write_document_dict(doc_dict, path=doc_path, client=client)
    return doc_dict[field]

