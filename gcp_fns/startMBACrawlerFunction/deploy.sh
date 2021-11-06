gcloud functions deploy startMBACrawlerFunction \
--runtime python37 \
--entry-point start_crawler \
--trigger-topic start-mba-crawler-pubsub \
--region europe-west3 --project=mba-pipeline \
 --timeout=540s