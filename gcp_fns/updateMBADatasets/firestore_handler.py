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
                    if "plot_x_price" in df_dict and df_dict["plot_x_price"] != None:
                        df_dict["plot_x_price"] = df_dict["plot_x_price"].split(",")
                    if "plot_y_price" in df_dict and df_dict["plot_y_price"] != None:
                        df_dict["plot_y_price"] = df_dict["plot_y_price"].split(",") 
                    document_id = df_dict[product_id_column]
                    
                    df_dict.update({'timestamp': datetime.datetime.now()})

                    doc_ref = self.db.collection(self.collection_name).document(document_id)
                    # doc = doc_ref.get()

                    # if doc.exists:
                    #     # TODO: if every property will be deleted which is not in df_dict its might be the same as just using set
                    #     # DELETE PROPERTIES which are not up to date anymore
                    #     properties = list(doc._data.keys())
                    #     properties_not_in_dict = list(np.setdiff1d(properties,list(df_dict.keys())))
                    #     for property_delete in properties_not_in_dict:
                    #         df_dict.update({property_delete: firestore.DELETE_FIELD})

                    #     # add content data
                    #     batch.update(doc_ref, df_dict)
                    # else:
                    # create new document with content data
                    batch.set(doc_ref, df_dict)
                except Exception as e:
                    print(str(e))
                    raise e
            print("Batch: {} of {}".format(str(k + 1), batch_count))
            batch.commit()

    def delete_by_df_batch(self, df, product_id_column, batch_size=500):
        print("Start delete firestore by batches")
        batch_count = int(len(df)/batch_size)
        for k, df_batch in df.groupby(np.arange(len(df))//batch_size):
            batch = self.db.batch()
            for i, df_row in df_batch.iterrows():
                try:
                    df_dict = df_row.to_dict()
                    document_id = df_dict[product_id_column]
                    doc_ref = self.db.collection(self.collection_name).document(document_id)
                    batch.delete(doc_ref)

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

    def get_all_docs(self):
        return self.db.collection(self.collection_name).stream()

    def get_document(self, doc_id):
        return self.db.collection(self.collection_name).document(doc_id).get()

    def delete_document(self, doc_id):
        self.db.collection(self.collection_name).document(doc_id).delete()

