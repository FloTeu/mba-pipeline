import firebase_admin
from datetime import datetime, date
from firebase_admin import credentials
from firebase_admin import firestore
from os.path import join
import numpy as np
from numpy.lib.index_tricks import _fill_diagonal_dispatcher
from mwfunctions.transform import get_shortened_plot_data, df_dict2subcollections
from mwfunctions.pydantic.firestore.mba_shirt_classes import FSMBAShirt
from mwfunctions.pydantic.firestore.firestore_classes import date2str
#from utils_plot import get_shortened_plot_data
import time

# Use the application default credentials
cred = credentials.ApplicationDefault()
class Firestore():
    def __init__(self, collection_name, marketplace, project="merchwatch"):
        try:
            app = firebase_admin.initialize_app(cred, {
            'projectId': project,
            }, name=project)
            self.db = firestore.client(app=app)
        except Exception as e:
            app = firebase_admin.get_app(project)
            self.db = firestore.client(app=app)
        self.collection_name = collection_name
        self.marketplace = marketplace

    def update_by_df(self, df, product_id_column):
        for i, df_row in df.iterrows():
            df_dict = df_row.to_dict()
            if "plot_x" in df_dict:
                df_dict["plot_x"] = df_dict["plot_x"].split(",")
            if "plot_y" in df_dict:
                df_dict["plot_y"] = df_dict["plot_y"].split(",") 
            document_id = df_dict[product_id_column]
            self.add_or_update_content(df_dict, document_id)


    def set_dict_to_batch(self, batch, doc_ref, fs_dict, doc_id):
        # TODO: Currently all years are saved. In future only current year need to be set, because older years do not update anymore
        # set firestore niche data
        if "subcollections" in fs_dict:
            subcollections = fs_dict["subcollections"]
            del fs_dict["subcollections"]
        
            # set subcollections
            for subcollection_id in subcollections.keys():
                subcollection_data = subcollections[subcollection_id]
                for subcollection_data_key in subcollection_data.keys():
                    subcollection_doc_data = subcollection_data[subcollection_data_key]
                    col_path = f"{self.collection_name}/{doc_id}/{subcollection_id}"
                    #print(col_path, subcollection_data_key)
                    doc_sub_collection_ref = self.db.collection(col_path).document(str(subcollection_data_key))
                    # update to prevent loosing data like review score
                    #batch.update(doc_sub_collection_ref, subcollection_doc_data)
                    # TODO: what happens if update on not existent doc?
                    batch.set(doc_sub_collection_ref, subcollection_doc_data, merge=True)
        # merge in order to not remove crawling data
        batch.set(doc_ref, fs_dict, merge=True)

    def update_by_df_batch(self, df, product_id_column, batch_size=100):
        # Note: 400 maximum 500 writes allowed per request/batch. consider subcollections, too
        print("Start update firestore by batches")
        batch_count = int(len(df)/batch_size)
        for k,df_batch in df.groupby(np.arange(len(df))//batch_size):
            time_start = time.time()
            batch = self.db.batch()
            for i, df_row in df_batch.iterrows():
                try:
                    df_dict = df_row.to_dict()
                    try:
                        df_dict["subcollections"] = df_dict2subcollections(df_dict)
                        df_dict.update(get_shortened_plot_data(df_dict["subcollections"]))
                    except Exception as e:
                        print(str(e))
                        continue
                    # replaced by short lists
                    use_old_plot_data = False
                    if use_old_plot_data:
                        if "plot_x" in df_dict and df_dict["plot_x"] != None:
                            df_dict["plot_x"] = df_dict["plot_x"].split(",")
                        if "plot_y" in df_dict and df_dict["plot_y"] != None:
                            df_dict["plot_y"] = df_dict["plot_y"].split(",") 
                        if "plot_x_price" in df_dict and df_dict["plot_x_price"] != None:
                            df_dict["plot_x_price"] = df_dict["plot_x_price"].split(",")
                        if "plot_y_price" in df_dict and df_dict["plot_y_price"] != None:
                            df_dict["plot_y_price"] = df_dict["plot_y_price"].split(",") 
                    else:
                        df_dict.pop('plot_x', None)
                        df_dict.pop('plot_y', None)
                        df_dict.pop('plot_x_price', None)
                        df_dict.pop('plot_y_price', None)


                    document_id = df_dict[product_id_column]
                    # add upload_since_days field

                    #df_dict["upload_since_days"] = get_upload_since_days_field(upload_date=df_dict["upload_date"].date())
                    df_dict["upload_since_days_map"] = get_upload_since_days_dict(upload_date=df_dict["upload_date"].date())
                    df_dict.update({'timestamp': datetime.now()})

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

                    # parse to FS pydantic class
                    # BQ contains strings "None" which can not be parsed to date by pydantic
                    df_dict["takedown_date"] = df_dict["takedown_date"] if df_dict["takedown_date"] != "None" else None
                    df_dict["update_last"] = df_dict["update_last"] if df_dict["update_last"] != "None" else None
                    df_dict["marketplace"] = self.marketplace
                    df_dict["doc_id"] = df_dict["asin"]
                    fs_mba_shirt = FSMBAShirt.parse_obj(df_dict)
                    # prevent field like "listings" which are only set by crawler, to be overwritten by this function
                    fs_dict = fs_mba_shirt.dict(include=set(df_dict.keys()))
                    date2str(fs_dict)
                    self.set_dict_to_batch(batch, doc_ref, fs_dict, df_dict["asin"])
                    #batch.set(doc_ref, df_dict)
                except Exception as e:
                    print(str(e))
                    raise e
            print("Batch: {} of {}".format(str(k + 1), batch_count))
            batch.commit()
            print("elapsed time for one batch: %.2f" % (time.time() - time_start))

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
        df_row_dict.update({'timestamp': datetime.now()})

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



UPLOAD_SINCE_DAYS_LIST = [7,14,30,90,365]

def get_upload_since_days_field(upload_since_days: int = None, upload_date: date = None):
    assert upload_since_days != None or upload_date != None, "Either 'upload_since_days' or 'upload_date' must be provided"
    if upload_date:
        upload_since_days = (datetime.now().date() - upload_date).days
    try:
        return list(filter(lambda x: x >= upload_since_days, UPLOAD_SINCE_DAYS_LIST))[0]
    except Exception as e:
        # case upload_since_days no int or upload_since_days > 365
        return None

def get_upload_since_days_dict(upload_since_days: int = None, upload_date: date = None):
    assert upload_since_days != None or upload_date != None, "Either 'upload_since_days' or 'upload_date' must be provided"
    if upload_date:
        upload_since_days = (datetime.now().date() - upload_date).days
    upload_since_days_map = {}
    if type(upload_since_days) == int:
        is_true = list(filter(lambda x: x >= upload_since_days, UPLOAD_SINCE_DAYS_LIST))
        is_false = list(filter(lambda x: x < upload_since_days, UPLOAD_SINCE_DAYS_LIST))
        for is_true_i in is_true:
            upload_since_days_map[str(is_true_i)] = True
        for is_false_i in is_false:
            upload_since_days_map[str(is_false_i)] = False
    else:
        for is_false_i in UPLOAD_SINCE_DAYS_LIST:
            upload_since_days_map[str(is_false_i)] = False

    return upload_since_days_map