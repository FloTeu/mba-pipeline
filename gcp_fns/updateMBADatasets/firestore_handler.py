import firebase_admin
import datetime
from firebase_admin import credentials
from firebase_admin import firestore
from os.path import join
import numpy as np

# Use the application default credentials
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': 'merchwatch',
})

class Firestore():
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self.log_collection_name = '%s_logs' % (collection_name)
        self.db = firestore.client()

    def update_by_df(self, df, product_id_column):
        for i, df_row in df.iterrows():
            df_dict = df_row.to_dict()
            if "plot_x" in df_dict:
                df_dict["plot_x"] = df_dict["plot_x"].split(",")
            if "plot_y" in df_dict:
                df_dict["plot_y"] = df_dict["plot_y"].split(",") 
            document_id = df_dict[product_id_column]
            self.add_or_update_content(df_dict, document_id)

    def update_by_df_batch(self, df, product_id_column, batch_size=500):
        print("Start update firestore by batches")
        batch_count = int(len(df)/batch_size)
        for k,df_batch in df.groupby(np.arange(len(df))//batch_size):
            batch = self.db.batch()
            for i, df_row in df_batch.iterrows():
                try:
                    df_dict = df_row.to_dict()
                    if "plot_x" in df_dict and df_dict["plot_x"] != None:
                        df_dict["plot_x"] = df_dict["plot_x"].split(",")
                    if "plot_y" in df_dict and df_dict["plot_y"] != None:
                        df_dict["plot_y"] = df_dict["plot_y"].split(",") 
                    document_id = df_dict[product_id_column]
                    
                    df_dict.update({'timestamp': datetime.datetime.now()})

                    doc_ref = self.db.collection(self.collection_name).document(document_id)
                    doc = doc_ref.get()

                    if doc.exists:
                        # DELETE PROPERTIES which are not up to date anymore
                        properties = list(doc._data.keys())
                        properties_not_in_dict = list(np.setdiff1d(properties,list(df_dict.keys())))
                        for property_delete in properties_not_in_dict:
                            df_dict.update({property_delete: firestore.DELETE_FIELD})

                        # add content data
                        batch.update(doc_ref, df_dict)
                    else:
                        # create new document with content data
                        batch.set(doc_ref, df_dict)
                except Exception as e:
                    print(str(e))
                    raise e
            print("Batch: {} of {}".format(str(k + 1), batch_count))
            batch.commit()

    def add_or_update_content(self, df_row_dict, document_id):
        # update lists in dict to firestore ArrayUnion
        #for key, value in df_row_dict.items():
        #    if type(value) == list:
        #        df_row_dict[key] = firestore.firestore.ArrayUnion(value)
        df_row_dict.update({'timestamp': datetime.datetime.now()})

        doc_ref = self.db.collection(self.collection_name).document(document_id)
        doc = doc_ref.get()

        if doc.exists:
            # add content data
            doc_ref.update(df_row_dict)
        else:
            # create new document with content data
            doc_ref.set(df_row_dict)

    def add_crawling_job(self, job):
        crawling_id = job.crawling_ID
        job_dict = job.asdict()
        del job_dict['crawling_ID']

        doc_ref = self.db.collection(self.log_collection_name).document(crawling_id)

        doc_ref.set(job_dict)

    def get_images_to_crawl(self):
        # TODO: in future it should be prevented to pull all firestore documents (those were images have been crawled should be excluded)
        return self.db.collection(self.collection_name).where(u'have_images_been_crawled', u'==', False).stream()

    def get_storage_data(self, image_paths, item):
        storage_data = []
        for image_path in image_paths:
            file_name = image_path.split("/")[-1]
            image_uuid = file_name.split(".")[0]
            relative_path = "{}/images/{}/{}".format(item["spider_category"], item["website_name"], image_path)
            url = join("https://storage.cloud.google.com/",item["bucket_name"],relative_path)
            bucket_path = join("gs://",item["bucket_name"],relative_path)
            storage_data.append({"file_name": file_name, "image_uuid":image_uuid, "relative_path":relative_path, "storage_url":url, "bucket_path":bucket_path})
        return storage_data

    def flag_have_images_been_crawled_true(self, product_id, image_paths, item):
        storage_data = self.get_storage_data(image_paths, item)
        image_uuids = [storage_dict["image_uuid"] for storage_dict in storage_data]
        image_urls = item["image_urls"]
        if len(image_uuids) != len(image_urls):
            print("Image_uuids and image_urls need to have same length")
        doc_ref = self.db.collection(self.collection_name).document(product_id)
        doc = doc_ref.get()
        # update image data in firestore
        # TODO: Just a workaround for breuninger. In future multiple img_urls need to be handled.
        '''        
        website_image_url = item["image_urls"][0]
        image_urls = []
        for i, image_url in enumerate(doc.get("image_urls")):
            try:
                if image_url == website_image_url:
                    image_url.update(storage_data[0])
            except Exception as e:
                pass
            image_urls.append(image_url)
        # workaround for modepark
        try:
            storage_data_fs = doc.get("storage_data")
            storage_data_fs.append(storage_data[0])
        except: 
            storage_data_fs = firestore.firestore.ArrayUnion(storage_data)
        '''
        if doc.exists:
            doc._data.update({u'image_uuids': image_uuids})
            doc._data.update({u'image_urls': image_urls})
            doc._data.update({u'storage_data': storage_data})
            doc._data.update({u'have_images_been_crawled': True})
            doc._data.update({u'processed_to_bigquery': False})

            # add content data
            doc_ref.update(doc._data)
        else:
            item_dict = {u'image_uuids': image_uuids, u'image_urls': image_urls, u'storage_data': storage_data, u'have_images_been_crawled': True, u'processed_to_bigquery': False}
            # create new document with content data
            doc_ref.set(item_dict)

    def get_all_docs(self):
        return self.db.collection(self.collection_name).stream()

    def get_document(self, doc_id):
        return self.db.collection(self.collection_name).document(doc_id).get()

    def delete_document(self, doc_id):
        self.db.collection(self.collection_name).document(doc_id).delete()

