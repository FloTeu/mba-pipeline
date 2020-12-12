    def insert_df_to_datastore(self, df, kind):
        dclient = datastore.Client()
        # The kind for the new entity
        columns = df.columns.values
        entities = []
        row_count = len(df)
        for i, row in df.iterrows():
            if i % 1000 == 0:
                print("row {} of {}".format(i, row_count))
            modulo = ((i+1) % 500)
            if modulo != 0:
                # The Cloud Datastore key for the new entity
                task_key = dclient.key(kind, row["asin"])
                # Prepares the new entity
                entity = datastore.Entity(key=task_key)

                for column in columns:
                    if column != "plot":
                        entity[column] = row[column]
                entities.append(entity)
            else:
                # Saves the entity
                try:
                    if i != 0 and len(entities) > 0: 
                        dclient.put_multi(entities)
                except Exception as e:
                    print(str(e))
                    raise e
                entities = []

    def update_datastore(self, marketplace, kind, dev=False, update_all=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        df = self.get_shirt_dataset(marketplace, dev=dev, update_all=update_all)
        self.insert_df_to_datastore(df, kind + dev_str)
        df = self.get_shirt_dataset_404(marketplace, dev=dev)
        self.delete_list_asin_from_datastore(marketplace, df["asin"].drop_duplicates().tolist(), dev=dev)

    def get_shirt_dataset_404_sql(self, marketplace, dev=False):
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"

        SQL_STATEMENT = """
        SELECT DISTINCT asin FROM `mba-pipeline.mba_{0}.merchwatch_shirts{1}` where price_last = 404
        
        """.format(marketplace, dev_str)

        return SQL_STATEMENT

    def get_shirt_dataset_404(self, marketplace, dev=False):
        shirt_sql = self.get_shirt_dataset_404_sql(marketplace, dev=dev)
        try:
            df_shirts=pd.read_gbq(shirt_sql, project_id="mba-pipeline")
        except Exception as e:
            print(str(e))
            raise e
        return df_shirts

    def delete_list_asin_from_datastore(self, marketplace, list_asin, dev=False):
        """
            Remove all given asins from datastore
        """
        dclient = datastore.Client()
        # if development than bigquery operations should only change dev tables
        dev_str = ""
        if dev:
            dev_str = "_dev"
        kind = marketplace + "_shirts" + dev_str
        list_keys = []
        list_keys_i = []
        for i, asin in enumerate(list_asin):
            if (i+1) % 500 == 0:
                list_keys.append(list_keys_i)
                list_keys_i = []
            list_keys_i.append(datastore.key.Key(kind, asin, project="mba-pipeline"))
            print("Delete key with asin: " + str(asin))
        list_keys.append(list_keys_i)
        for list_keys_i in list_keys:
            dclient.delete_multi(list_keys_i)