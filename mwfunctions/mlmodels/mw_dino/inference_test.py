# Copyright (c) Facebook, Inc. and its affiliates.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copy/Customization of eval_linear.py 
import argparse
import json
import os
from pathlib import Path
import tqdm

import numpy as np
import torch
from torch import nn
import torch.backends.cudnn as cudnn
from torchvision import transforms as pth_transforms

from mvfunctions.mvmlflow import MVMLFlow
from mvfunctions import environment
from mvfunctions.ml_pipeline.pytorch.df_dataset import TorchDataframeImageInferenceDataset
from mvfunctions.ml_pipeline.pytorch.loss_functions import ArcMarginProduct
from mvfunctions.config import primitive_dict
from mvfunctions.constants import ml_constants as mlc
from mvfunctions.bot import send

from preprocessing import get_train_transform, get_val_transform

from model import XcitClassifierHeadModelCuda

import utils

from contextlib import nullcontext

from reco_hub.project_code.data_classes.ImageJson import ImageJson


def eval_linear(args):

    # init args.gpu and stuff
    utils.init_distributed_mode(args)
    print("git:\n  {}\n".format(utils.get_sha()))
    print("\n".join("%s: %s" % (k, str(v)) for k, v in sorted(dict(vars(args)).items())))
    cudnn.benchmark = True

    assert args.mlflow_run_url, "Only working on existing mlflow_run"

    # Only log on master process
    # environment.assert_gcp_auth(args.credentials)
    with MVMLFlow(experiments_root_dir=args.experiments_root_dir,
                        mlflow_run_url=args.mlflow_run_url,
                        local_rank=args.gpu,
                        global_rank=torch.distributed.get_rank(),
                        restore_training_context=True) as mvflow:

                
        # ============ preparing data ... ============
        val_transform = get_val_transform()

        inference_dataset = TorchDataframeImageInferenceDataset(images_root_dir=args.images_root_dir, src=args.test_csv_path, relative_path_col=args.relative_path_col, columns=None, transform=val_transform)
        inference_loader = torch.utils.data.DataLoader(
            inference_dataset,
            batch_size=args.batch_size_per_gpu,
            num_workers=args.num_workers,
            pin_memory=True,
        )
        # print(f"Data loaded with {len(dataset_train)} train and {len(dataset_val)} val imgs.")

        # ============ building network ... ============
        classif_model = XcitClassifierHeadModelCuda(arch=args.arch, 
            patch_size=args.patch_size, 
            n_last_blocks=args.n_last_blocks, 
            avgpool_patchtokens=args.avgpool_patchtokens, 
            backbone_weights_pth=args.pretrained_weights, 
            checkpoint_key=args.checkpoint_key, 
            device_id=args.gpu)
        
        classif_model.eval()


        # Optionally resume from a checkpoint
        to_restore = {"epoch": 0, "best_acc": 0.}
        chk_path = f"{mvflow.get_checkpoint_dir()}/checkpoint.pth"
        utils.restart_from_checkpoint(
            chk_path,
            #os.path.join(args.output_dir, "checkpoint.pth.tar"),
            run_variables=to_restore,
            state_dict=classif_model
        )
        # start_epoch = to_restore["epoch"]
        # best_acc = to_restore["best_acc"]

        fake_dataset_id = os.path.basename(args.test_csv_path)[:-4]
        json_saver = JsonSaver(mvflow=mvflow, dataset_id=fake_dataset_id)

        # TODO: geht hier tqdm einfach?
        for stuff in tqdm.tqdm(inference_loader):
            inp, target, image_uuids, product_ids, gs_urls = stuff
            inp, target = inp.cuda(non_blocking=True), target.cuda(non_blocking=True)
            logits = classif_model(inp)
            for i, logit in enumerate(logits):
                image_json = ImageJson(image_uuid=image_uuids[i],
                    product_id=product_ids[i],
                    gs_url=gs_urls[i],
                    descriptor= np.array(logit))
                json_saver.save(json_dict=image_json.json_dict, image_uuid=image_uuids[i])
                stop = 1

class JsonSaver(object):
    def __init__(self, mvflow, dataset_id) -> None:
        super().__init__()
        local_json_root_dir = f"{mvflow.experiment_dir}/inference_data/{dataset_id}_{mvflow.run_id}_jsons/"
        # Appendix from dflow
        self.local_json_dir = f"{local_json_root_dir}/dataflow/json/eval/{dataset_id}/{mvflow.run_id}"
        os.makedirs(self.local_json_dir, exist_ok=True)
    
    def save(self, json_dict, image_uuid):
        with open(f"{self.local_json_dir}/{image_uuid}.json", "w") as f:
                json.dump(json_dict, f)


def train(classif_model, optimizer, classif_loss_fn, loader, epoch):
    classif_model.linear_classifier.train()
    classif_loss_fn.train()

    metric_logger = utils.MetricLogger(delimiter="  ").add_meter('lr', utils.SmoothedValue(window_size=1, fmt='{value:.6f}'))

    header = 'Epoch: [{}]'.format(epoch)
    for (inp, target) in metric_logger.log_every(loader, 20, header):
        inp, target = inp.cuda(non_blocking=True), target.cuda(non_blocking=True)
        
        logits = classif_model(inp)
        # loss = nn.CrossEntropyLoss()(output, target)
        _, loss = classif_loss_fn(logits, target)
        
        # compute the gradients
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        # log 
        torch.cuda.synchronize()
        metric_logger.update(loss=loss.item(), lr=optimizer.param_groups[0]["lr"])
        # TODO: delete if it works
        # metric_logger.update(lr=optimizer.param_groups[0]["lr"])
    # gather the stats from all processes
    metric_logger.synchronize_between_processes()
    print("Averaged stats:", metric_logger)
    return {k: meter.global_avg for k, meter in metric_logger.meters.items()}


@torch.no_grad()
def validate_network(val_loader, classif_model, classif_loss_fn: ArcMarginProduct):
    classif_model.linear_classifier.eval()
    classif_loss_fn.eval()

    metric_logger = utils.MetricLogger(delimiter="  ")
    header = 'Test:'
    for inp, target in metric_logger.log_every(val_loader, 20, header):
        inp, target = inp.cuda(non_blocking=True), target.cuda(non_blocking=True)

        logits = classif_model(inp)

        # loss = nn.CrossEntropyLoss()(output, target)
        logits, loss = classif_loss_fn(logits, target)

        if classif_loss_fn.module.out_features >= 5:
            acc1, acc5 = utils.accuracy(logits, target, topk=(1, 5))
        else:
            acc1, = utils.accuracy(logits, target, topk=(1,))

        batch_size = inp.shape[0]
        metric_logger.update(loss=loss.item())
        metric_logger.meters['acc1'].update(acc1.item(), n=batch_size)
        if classif_loss_fn.module.out_features >= 5:
            metric_logger.meters['acc5'].update(acc5.item(), n=batch_size)
    if classif_loss_fn.module.out_features >= 5:
        print('* Acc@1 {top1.global_avg:.3f} Acc@5 {top5.global_avg:.3f} loss {losses.global_avg:.3f}'
          .format(top1=metric_logger.acc1, top5=metric_logger.acc5, losses=metric_logger.loss))
    else:
        print('* Acc@1 {top1.global_avg:.3f} loss {losses.global_avg:.3f}'
          .format(top1=metric_logger.acc1, losses=metric_logger.loss))
    return {k: meter.global_avg for k, meter in metric_logger.meters.items()}


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Evaluation with linear classification on ImageNet')
    parser.add_argument('--n_last_blocks', default=4, type=int, help="""Concatenate [CLS] tokens
        for the `n` last blocks. We use `n=4` when evaluating ViT-Small and `n=1` with ViT-Base.""")
    parser.add_argument('--avgpool_patchtokens', default=False, type=utils.bool_flag,
        help="""Whether ot not to concatenate the global average pooled features to the [CLS] token.
        We typically set this to False for ViT-Small and to True with ViT-Base.""")
    parser.add_argument('--arch', default='vit_small', type=str,
        choices=['vit_tiny', 'vit_small', 'vit_base', "xcit_small_12_p16"], help='Architecture (support only ViT atm).')
    parser.add_argument('--patch_size', default=16, type=int, help='Patch resolution of the model.')
    parser.add_argument('--pretrained_weights', default='', type=str, help="Path to pretrained weights to evaluate.")
    parser.add_argument("--checkpoint_key", default="teacher", type=str, help='Key to use in the checkpoint (example: "teacher")')
    parser.add_argument('--epochs', default=100, type=int, help='Number of epochs of training.')
    parser.add_argument("--lr", default=0.001, type=float, help="""Learning rate at the beginning of
        training (highest LR used during training). The learning rate is linearly scaled
        with the batch size, and specified here for a reference batch size of 256.
        We recommend tweaking the LR depending on the checkpoint evaluated.""")
    parser.add_argument('--batch_size_per_gpu', default=128, type=int, help='Per-GPU batch-size')
    parser.add_argument("--dist_url", default="env://", type=str, help="""url used to set up
        distributed training; see https://pytorch.org/docs/stable/distributed.html""")
    parser.add_argument("--local_rank", default=0, type=int, help="Please ignore and do not set this argument.")
    parser.add_argument('--data_path', default='/path/to/imagenet/', type=str)
    parser.add_argument('--num_workers', default=10, type=int, help='Number of data loading workers per GPU.')
    parser.add_argument('--val_freq', default=1, type=int, help="Epoch frequency for validation.")
    parser.add_argument('--output_dir', default=".", help='Path to save logs and checkpoints')
    parser.add_argument('--num_labels', default=1000, type=int, help='Number of labels for linear classifier')

    # MV Arguments
    parser.add_argument('--images_root_dir', required=True, type=str, help='Root dir of relative paths to images.')
    parser.add_argument('--train_csv_path', required=True, type=str, help='Path to train csv file.')
    parser.add_argument('--test_csv_path', required=True, type=str, help='Path to test csv file.')
    parser.add_argument('--relative_path_col', default="relative_path", type=str, help='Path to .')

    # MVMLFlow stuff
    parser.add_argument('--experiments_root_dir', required=True, type=str, help="Gcspath relative col name")
    parser.add_argument('--experiment_name', type=str, default=None, help="Select a subset with replacement")
    parser.add_argument('--run_name', type=str, default=None,
                        help="Select a subset with replacement")
    parser.add_argument('--mlflow_run_url', type=str, default=None,
                        help="Select a subset with replacement")
    parser.add_argument('--credentials', type=str, default=None,
                        help="Select a subset with replacement")

    args = parser.parse_args()
    eval_linear(args)
