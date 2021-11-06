export GOOGLE_CLOUD_PROJECT=mba-pipeline;
gcloud functions deploy startMBACrawlerFunction \
--runtime python37 \
--entry-point start_crawler \
--trigger-topic start-mba-crawler-pubsub \
--region europe-west3 --project=$GOOGLE_CLOUD_PROJECT \
 --timeout=540s --set-env-vars GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT