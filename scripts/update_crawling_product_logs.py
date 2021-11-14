import pandas as pd
from mwfunctions.cloud.firestore import write_document_dict, does_document_exists, create_client
from google.cloud import bigquery
from mwfunctions.environment import get_gcp_project
from mwfunctions.pydantic.crawling_classes import CrawlingType
from mwfunctions.pydantic.firestore.crawling_log_classes import FSMBACrawlingProductLogsSubcollectionDoc

def get_asin_crawled(table_id, bq_client):
    '''
        Returns a unique list of asins that are already crawled
    '''
    try:
        list_asin = \
        bq_client.query("SELECT asin FROM " + table_id + " group by asin").to_dataframe().drop_duplicates(
            ["asin"])["asin"].tolist()
    except Exception as e:
        print(str(e))
        list_asin = []
    return list_asin

bq_client = bigquery.Client(project=get_gcp_project())
fs_client = create_client(project="merchwatch")

for marketplace in ["de", "com"]:
    #products_already_crawled = get_asin_crawled(f"mba_{marketplace}.products", bq_client)
    #products_already_crawled_tmp = products_already_crawled[-98210:len(products_already_crawled)]

    if False:
        with open(f'{marketplace}_products_already_crawled.txt') as f:
            products_already_crawled = f.read().splitlines()

        index_start = 39900 + 122700 + 30400
        products_already_crawled = products_already_crawled[
                                             index_start:len(products_already_crawled)]

        for i, asin in enumerate(products_already_crawled):
            if i % 100 == 0:
                print(f"{i} of {len(products_already_crawled)} overview {marketplace}")
            fs_col_path = f"crawling_product_logs/{marketplace}/{CrawlingType.OVERVIEW}"
            fs_doc_path = f"{fs_col_path}/{asin}"
            if not does_document_exists(fs_doc_path, client=fs_client):
                doc = FSMBACrawlingProductLogsSubcollectionDoc(doc_id=asin)
                doc.set_fs_col_path(fs_col_path)
                doc.update_timestamp()
                doc.write_to_firestore(client=fs_client)

    if marketplace == "com":
        with open(f'{marketplace}_products_images_already_downloaded.txt') as f:
            products_images_already_downloaded = f.read().splitlines()

        index_start = 147500 + 700
        products_images_already_downloaded = products_images_already_downloaded[index_start:len(products_images_already_downloaded)]

        # products_images_already_downloaded = get_asin_crawled(
        #     f"mba_{marketplace}.products_images", bq_client)  # if not self.debug else []

        for i, asin in enumerate(products_images_already_downloaded):
            if i % 100 == 0:
                print(f"{i} of {len(products_images_already_downloaded)} image {marketplace}")
            fs_col_path = f"crawling_product_logs/{marketplace}/{CrawlingType.IMAGE}"
            fs_doc_path = f"{fs_col_path}/{asin}"
            if not does_document_exists(fs_doc_path, client=fs_client):
                doc = FSMBACrawlingProductLogsSubcollectionDoc(doc_id=asin)
                doc.set_fs_col_path(fs_col_path)
                doc.write_to_firestore(client=fs_client)

