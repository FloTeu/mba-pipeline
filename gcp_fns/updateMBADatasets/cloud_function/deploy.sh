gcloud functions deploy updateMBADatasets \
--runtime python37 \
--entry-point updateBqShirtTables \
--trigger-topic update-mba-datasets-pubsub \
--region europe-west3 --project=mba-pipeline