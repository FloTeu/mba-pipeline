import numpy as np
import torchvision

from sagemaker.pytorch.model import PyTorchPredictor

from mwfunctions.image.visualize.visualize_fns import imshow
from mwfunctions.mlmodels.pytorch_train_and_deploy.constants import AWS_CIFAR_ENDPOINT_NAME
from mwfunctions.mlmodels.pytorch_train_and_deploy.dataloader import get_train_test_loader, classes, batch_size_default

predictor = PyTorchPredictor(endpoint_name=AWS_CIFAR_ENDPOINT_NAME)

trainloader, testloader = get_train_test_loader()

# get some random training images
dataiter = iter(testloader)
images, labels = dataiter.next()

print(type(images), images.shape)
image_list = images.tolist()
aws_input_data = {"inputs": image_list}
predictions = predictor.predict(aws_input_data)

# show images
imshow(torchvision.utils.make_grid(images))
# print labels
print(' '.join(f'pred: {classes[np.argmax(predictions[j])]:5s}, true: {classes[labels[j]]:5s}' for j in range(batch_size_default)))





