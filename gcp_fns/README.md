
## Create Google Cloud Functions
cd mba-pipeline/gcp_fns/updateMBADatasets
gcloud functions deploy updateMBADatasets --runtime python37 --trigger-topic update-mba-datasets-pubsub --region europe-west3 --entry-point updateBqShirtTables


## Create Google Pub Sub
gcloud pubsub topics create update-mba-datasets-pubsub

## Create Google Scheduler
gcloud scheduler jobs create pubsub update-mba-datasets-daily --schedule="1 0 * * *" --topic=update-mba-datasets-pubsub --message-body "Hello" --time-zone "Europe/Berlin" --description "Schedular which updates mba datasets once a day"