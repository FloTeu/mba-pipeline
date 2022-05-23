# creates model.tar.gz file
# must include .pth and model.py file for execution of inference.py

tar zcvf deployment/model.tar.gz train/cifar_net.pth model.py
