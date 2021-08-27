from torchvision import transforms as pth_transforms


# Our images are way larger then 224 -> resize to 256 (imagenet size)
# TODO: This depends on the dataset
def get_train_transform():
    return pth_transforms.Compose([
        # Resize keeps aspect ratio
        pth_transforms.Resize(224, interpolation=3, max_size=256),
        #pth_transforms.Resize(256+25, interpolation=3),
        # pth_transforms.CenterCrop(256),
        # Always squared images
        pth_transforms.RandomResizedCrop(224, scale=(0.1, 1)),
        pth_transforms.RandomHorizontalFlip(),
        pth_transforms.ToTensor(),
        pth_transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
    ])


def get_val_transform():
    return pth_transforms.Compose([
        pth_transforms.Resize(224, interpolation=3, max_size=256),
        pth_transforms.CenterCrop(224),
        pth_transforms.ToTensor(),
        pth_transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
    ])

# def get_train_transform_aby():
#     return pth_transforms.Compose([
#         # Resize keeps aspect ratio
#     pth_transforms.Resize(256+25, interpolation=3),
#     pth_transforms.CenterCrop(256),
#     # Always squared images
#     pth_transforms.RandomResizedCrop(224),
#     pth_transforms.RandomHorizontalFlip(),
#     pth_transforms.ToTensor(),
#     pth_transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
# ])

# def get_inference_transform():
#     return pth_transforms.Compose([
#     pth_transforms.Resize(256+25, interpolation=3),  # 281
#     pth_transforms.CenterCrop(224),
#     pth_transforms.ToTensor(),
#     pth_transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
# ])
