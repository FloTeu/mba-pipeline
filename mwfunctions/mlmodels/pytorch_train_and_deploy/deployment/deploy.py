import os

import numpy as np
from mwfunctions.mlmodels.pytorch_train_and_deploy.constants import SAGEMAKER_ROLE, S3_MODEL_DATA_PATH, AWS_CIFAR_ENDPOINT_NAME, AWS_DAGEMAKER_INSTANCE_TYPE
from sagemaker.pytorch import PyTorchModel
from sagemaker.serializers import JSONSerializer
from sagemaker.deserializers import JSONDeserializer

model = PyTorchModel(
    entry_point="inference.py",
    #source_dir="code",
    role=SAGEMAKER_ROLE,
    model_data=S3_MODEL_DATA_PATH,
    framework_version="1.5.0",
    py_version="py3",
)



# set local_mode to False if you want to deploy on a remote
# SageMaker instance

local_mode = False

if local_mode:
    instance_type = "local"
else:
    instance_type = AWS_DAGEMAKER_INSTANCE_TYPE

predictor = model.deploy(
    endpoint_name=AWS_CIFAR_ENDPOINT_NAME,
    initial_instance_count=1,
    instance_type=instance_type,
    serializer=JSONSerializer(),
    deserializer=JSONDeserializer(),
)

dummy_data = {"inputs": np.random.rand(16, 3, 32, 32).tolist()}

# t = 0
#
# if not local_mode:
#     predictor.delete_endpoint()
# else:
#     os.system("docker container ls | grep 8080 | awk '{print $1}' | xargs docker container rm -f")