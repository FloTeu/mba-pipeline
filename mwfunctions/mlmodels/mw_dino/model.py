import sys
import torch
import torch.nn as nn
import vision_transformer as vits
from torchvision import models as torchvision_models

from typing import Optional

import utils


class XcitClassifierHeadModelCuda(nn.Module):
    def __init__(self,
                 arch,
                 patch_size: int = 16,
                 n_last_blocks: int = 4,
                 avgpool_patchtokens: bool = False,
                 backbone_weights_pth: Optional[str] = None,
                 checkpoint_key: Optional[str] = None,
                 device_id: int = 0,
                 is_ddp: bool = True):
        """[summary]

        Notes:
            Whats n_last_blocks?

        Args:
            arch ([type]): [description]
            patch_size ([type]): [description]
            n_last_blocks ([type]): [description]
            avgpool_patchtokens ([type]): [description]
            pretrained_weights ([type]): [description]
            checkpoint_key ([type]): [description]
            device_ids ([type]): [description]
        """

        super().__init__()

        self.arch = arch
        self.patch_size = patch_size
        self.n_last_blocks = n_last_blocks
        self.avgpool_patchtokens = avgpool_patchtokens
        self.backbone_weights_pth = backbone_weights_pth
        self.checkpoint_key = checkpoint_key
        self.device_id = device_id

        # ============ building network ... ============
        # if the network is a Vision Transformer (i.e. vit_tiny, vit_small, vit_base)
        if self.arch in vits.__dict__.keys():
            backbone = vits.__dict__[self.arch](
                patch_size=self.patch_size, num_classes=0)
            embed_dim = backbone.embed_dim * \
                (self.n_last_blocks + int(self.avgpool_patchtokens))
        # if the network is a XCiT
        elif "xcit" in self.arch:
            backbone = torch.hub.load(
                'facebookresearch/xcit', self.arch, num_classes=0)
            embed_dim = backbone.embed_dim
        # otherwise, we check if the architecture is in torchvision models
        elif self.arch in torchvision_models.__dict__.keys():
            backbone = torchvision_models.__dict__[self.arch]()
            embed_dim = backbone.fc.weight.shape[1]
            backbone.fc = nn.Identity()
        else:
            print(f"Unknow architecture: {self.arch}")
            sys.exit(1)

        # model = vits.__dict__[args.arch](patch_size=args.patch_size, num_classes=0)
        backbone.cuda()
        backbone.eval()

        print(f"Model {self.arch} {self.patch_size}x{self.patch_size} built.")
        # load weights to evaluate
        utils.load_pretrained_weights(
            backbone, self.backbone_weights_pth, self.checkpoint_key, self.arch, self.patch_size)

        self.backbone = backbone

        # TODO: Was sind n_last_blocks?
        # TODO: args.avgpool_patchtokens?
        # TODO; output_dim
        # wanted_embedding_dim = 384
        # linear_classifier = LinearClassifier(model.embed_dim * (args.n_last_blocks + int(args.avgpool_patchtokens)), num_labels=args.num_labels)
        # Workaround
        # linear_classifier = LinearClassifier(embed_dim, num_labels=embed_dim)
        # linear_classifier = linear_classifier.cuda()
        # linear_classifier = nn.parallel.DistributedDataParallel(linear_classifier, device_ids=[args.gpu])


        linear_classifier = Embedder(embed_dim, num_labels=embed_dim)
        linear_classifier = linear_classifier.cuda()
        if utils.has_batchnorms(linear_classifier):
            linear_classifier = nn.SyncBatchNorm.convert_sync_batchnorm(linear_classifier)
        if is_ddp:
            linear_classifier = nn.parallel.DistributedDataParallel(
                linear_classifier, device_ids=[self.device_id])
        self.linear_classifier = linear_classifier

        self.embed_dim = embed_dim
        # self.num_labels = num_labels

    def forward(self, inp):
        # forward
        with torch.no_grad():
            if "vit" in self.arch:
                intermediate_output = self.backbone.get_intermediate_layers(
                    inp, self.args.n_last_blocks)
                output = [x[:, 0] for x in intermediate_output]
                if self.args.avgpool_patchtokens:
                    output.append(torch.mean(
                        intermediate_output[-1][:, 1:], dim=1))
                output = torch.cat(output, dim=-1)
            else:
                output = self.backbone(inp)
        output = self.linear_classifier(output)
        return output

    def load_state_dict(self, state_dict: 'OrderedDict[str, Tensor]', strict: bool):
        return self.linear_classifier.load_state_dict(state_dict, strict=strict)
        # return super().load_state_dict(state_dict, strict=strict)

    def state_dict(self):
        return self.linear_classifier.state_dict()


class LinearClassifier(nn.Module):
    """Linear layer to train on top of frozen features"""

    def __init__(self, dim, num_labels=1000):
        super(LinearClassifier, self).__init__()
        self.num_labels = num_labels
        self.linear = nn.Linear(dim, num_labels)
        self.linear.weight.data.normal_(mean=0.0, std=0.01)
        self.linear.bias.data.zero_()

    def forward(self, x):
        # flatten
        x = x.view(x.size(0), -1)

        # linear layer
        return self.linear(x)


class Embedder(nn.Module):
    """Linear layer to train on top of frozen features"""

    def __init__(self, dim, num_labels=1000):
        super(Embedder, self).__init__()
        self.num_labels = num_labels
        self.linear1 = nn.Linear(dim, num_labels)
        self.linear1.weight.data.normal_(mean=0.0, std=0.01)
        self.linear1.bias.data.zero_()

        # self.linear2 = nn.Linear(num_labels, num_labels)
        # self.linear2.weight.data.normal_(mean=0.0, std=0.01)
        # self.linear2.bias.data.zero_()
        # self.bn = nn.BatchNorm1d(num_labels)
        # self.bn2 = nn.BatchNorm1d(num_labels)
        # self.dropo = nn.Dropout(p=0.1)

    def forward(self, x):
        # flatten
        x = x.view(x.size(0), -1)

        # linear layer
        # x = self.dropo(x)
        x = self.linear1(x)
        # x = self.bn(x)
        # x = torch.relu(x)
        # x = self.linear2(x)
        # x = self.bn2(x)
        return x
