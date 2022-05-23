# import torch
# import torch.nn as nn
# import torchvision
# import torchvision.transforms as transforms
#
# from model import net
# from dataloader import get_train_test_loader
# from train import classes
# from mwfunctions.image.visualize.visualize_fns import imshow
#
# transform = transforms.Compose(
#     [transforms.ToTensor(),
#      transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])
#
# trainloader, testloader = get_train_test_loader(transform)
#
# dataiter = iter(testloader)
# images, labels = dataiter.next()
#
#
# # print images
# imshow(torchvision.utils.make_grid(images))
# print('GroundTruth: ', ' '.join(f'{classes[labels[j]]:5s}' for j in range(4)))