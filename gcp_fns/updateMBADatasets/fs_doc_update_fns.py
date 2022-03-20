
from mwfunctions.cloud.firestore import firestore_fns
from mwfunctions.cloud.firestore.commons import FSSimpleFilterQuery
from mwfunctions.pydantic.firestore.mba_shirt_classes import FSMBAShirt

def create_keywords_meaningful_if_not_set(marketplace, project="merchwatch", max_reads=10000):
    """ Function reads all (but maximal max_reads docs) of mba shirt docs for certain marketplace, which does not contain the list field keywords_meaningful.
        Updates/creates field keywords_meaningful for all fields with value None.
    """
    fs_client = firestore_fns.create_client(project, use_cache=False)
    simple_query_filters = [FSSimpleFilterQuery(field="keywords_meaningful", comparison_operator="==", value=None)]
    doc_snap_iter = firestore_fns.get_docs_snap_iterator(f"{marketplace}_shirts", simple_query_filters, client=fs_client)

    try:
        for i, doc_snap in enumerate(doc_snap_iter):
            if i % 1000 == 0:
                print(f"Iteration {i} with id: {doc_snap.id}")
            try:
                fs_mba_shirt = FSMBAShirt.parse_fs_doc_snapshot(doc_snap)
            except Exception as e:
                print(f"Could not parse fs_doc {doc_snap.id} of collection {marketplace}_shirts")
                continue
            fs_mba_shirt.set_keywords_meaningful_if_none()
            fs_mba_shirt.create_short_dicts_if_not_set()

            fs_mba_shirt.write_to_firestore()
            # if more than max_reads break up loop
            if (i+2) > max_reads:
                break
    except Exception as e:
        print("Error during iteration", e)
