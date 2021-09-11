# This script is only used for development. cloudbuild.yaml handles all ci/cd processes and replaces deploy.sh in production/demo.
cd ..
export GOOGLE_CLOUD_PROJECT=mba-pipeline;
export image_name=ml-model-pytorch-pre-dino;
# create new container image via cloud build
#gcloud builds submit . --tag eu.gcr.io/$GOOGLE_CLOUD_PROJECT/$image_name --project $GOOGLE_CLOUD_PROJECT --timeout=15m
# deploy cloud run
gcloud run deploy $image_name --port=8080 --image europe-west1-docker.pkg.dev/mba-pipeline/pytorch-models/ml_model_pytorch_pre_dino \
--platform managed --project $GOOGLE_CLOUD_PROJECT --cpu=4 --memory=2Gi --platform=managed --timeout=15m --set-env-vars GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT --region="europe-west1"

# europe-west1-docker.pkg.dev/$GOOGLE_CLOUD_PROJECT/pytorch-models/ml_model_pytorch_pre_dino \
cd scripts