import torchvision.transforms as transforms
import torchvision
import torch
import os

transform_default = transforms.Compose(
    [transforms.ToTensor(),
     transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])

batch_size_default = 4
FILE_DIR_PATH = "/".join(os.path.abspath(__file__).split("/")[:-1])

def get_train_test_loader(transform=transform_default, batch_size=batch_size_default):
    # download data
    trainset = torchvision.datasets.CIFAR10(root=FILE_DIR_PATH + '/data', train=True,
                                            download=True, transform=transform)
    trainloader = torch.utils.data.DataLoader(trainset, batch_size=batch_size,
                                              shuffle=True, num_workers=2)

    testset = torchvision.datasets.CIFAR10(root=FILE_DIR_PATH + '/data', train=False,
                                           download=True, transform=transform)
    testloader = torch.utils.data.DataLoader(testset, batch_size=batch_size,
                                             shuffle=False, num_workers=2)

    return trainloader, testloader


classes = ('plane', 'car', 'bird', 'cat',
           'deer', 'dog', 'frog', 'horse', 'ship', 'truck')