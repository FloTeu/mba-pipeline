from sagemaker.pytorch.model import PyTorchPredictor
from mwfunctions.mlmodels.pytorch_train_and_deploy.constants import AWS_CIFAR_ENDPOINT_NAME

predictor = PyTorchPredictor(endpoint_name=AWS_CIFAR_ENDPOINT_NAME)

predictor.delete_endpoint()