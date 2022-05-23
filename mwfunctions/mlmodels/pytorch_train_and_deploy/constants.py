SAGEMAKER_ROLE = 'arn:aws:iam::446618142480:role/SageMaker'
AWS_CIFAR_ENDPOINT_NAME = "cifar-deployment-test"
AWS_DAGEMAKER_INSTANCE_TYPE = "ml.c4.xlarge"

S3_MODEL_DATA_BUCKET = "sagemaker-eu-central-1-446618142480"
S3_MODEL_DATA_KEY = "DEMO-samples/cifar/model.tar.gz"
S3_MODEL_DATA_PATH = f"s3://{S3_MODEL_DATA_BUCKET}/{S3_MODEL_DATA_KEY}"