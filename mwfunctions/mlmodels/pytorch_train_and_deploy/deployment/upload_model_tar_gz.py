import boto3
from mwfunctions.mlmodels.pytorch_train_and_deploy.constants import SAGEMAKER_ROLE, S3_MODEL_DATA_PATH, S3_MODEL_DATA_BUCKET, S3_MODEL_DATA_KEY

s3 = boto3.resource('s3')
s3.Bucket(S3_MODEL_DATA_BUCKET).upload_file("model.tar.gz", S3_MODEL_DATA_KEY)