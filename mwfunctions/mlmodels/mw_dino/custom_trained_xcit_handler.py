import os
import torch
from mvfunctions.image.conversion import bytes2pil

from ts.torch_handler.base_handler import BaseHandler
import warnings
import torchvision
import utils
from preprocessing import get_val_transform

# Dino defaults (0.485, 0.456, 0.406), (0.229, 0.224, 0.225)

IMAGENET_DEFAULT_MEAN = (0.485, 0.456, 0.406)
IMAGENET_DEFAULT_STD = (0.229, 0.224, 0.225)

IMG_SIZE = 224

MODEL_NAME = os.environ["MODEL_NAME"]
# For now the model name should be enough
# BACKBONE = os.environ[]


# def get_test_transforms():
#     return torchvision.transforms.Compose([
#         torchvision.transforms.Resize(
#             (IMG_SIZE, IMG_SIZE)),
#         torchvision.transforms.ToTensor(),
#         torchvision.transforms.Normalize(
#             mean=IMAGENET_DEFAULT_MEAN,
#             std=IMAGENET_DEFAULT_STD,
#         )
#     ])


def load_pretrained_weights(model, pretrained_weights, checkpoint_key, model_name, patch_size):
    if os.path.isfile(pretrained_weights):
        state_dict = torch.load(pretrained_weights, map_location="cpu")
        if checkpoint_key is not None and checkpoint_key in state_dict:
            print(f"Take key {checkpoint_key} in provided checkpoint dict")
            state_dict = state_dict[checkpoint_key]
        # remove `module.` prefix
        state_dict = {k.replace("module.", ""): v for k,
                      v in state_dict.items()}
        # remove `backbone.` prefix induced by multicrop wrapper
        state_dict = {k.replace("backbone.", ""): v for k,
                      v in state_dict.items()}
        msg = model.load_state_dict(state_dict, strict=False)
        print('Pretrained weights found at {} and loaded with msg: {}'.format(
            pretrained_weights, msg))
    else:
        raise RuntimeError(
            f"Pretrained weights file {pretrained_weights} is not a file")
        # print("Please use the `--pretrained_weights` argument to indicate the path of the checkpoint to evaluate.")
        # url = None
        # if model_name == "vit_small" and patch_size == 16:
        # url = "dino_deitsmall16_pretrain/dino_deitsmall16_pretrain.pth"
        # elif model_name == "vit_small" and patch_size == 8:
        # url = "dino_deitsmall8_pretrain/dino_deitsmall8_pretrain.pth"
        # elif model_name == "vit_base" and patch_size == 16:
        # url = "dino_vitbase16_pretrain/dino_vitbase16_pretrain.pth"
        # elif model_name == "vit_base" and patch_size == 8:
        # url = "dino_vitbase8_pretrain/dino_vitbase8_pretrain.pth"
        # elif model_name == "xcit_small_12_p16":
        # url = "dino_xcit_small_12_p16_pretrain/dino_xcit_small_12_p16_pretrain.pth"
        # elif model_name == "xcit_small_12_p8":
        # url = "dino_xcit_small_12_p8_pretrain/dino_xcit_small_12_p8_pretrain.pth"
        # elif model_name == "xcit_medium_24_p16":
        # url = "dino_xcit_medium_24_p16_pretrain/dino_xcit_medium_24_p16_pretrain.pth"
        # elif model_name == "xcit_medium_24_p8":
        # url = "dino_xcit_medium_24_p8_pretrain/dino_xcit_medium_24_p8_pretrain.pth"
        # if url is not None:
        # print("Since no pretrained weights have been provided, we load the reference pretrained DINO weights.")
        # state_dict = torch.hub.load_state_dict_from_url(url="https://dl.fbaipublicfiles.com/dino/" + url)
        # model.load_state_dict(state_dict, strict=True)
        # else:
        # print("There is no reference weights available for this model => We use random weights.")


class ModelHandler(BaseHandler):
    def initialize(self, context):
        """
        Initialize model. This will be called during model loading time
        :param context: Initial context contains model server system properties.
        :return:
        """

        self._context = context
        self.manifest = context.manifest
        properties = context.system_properties
        model_dir = properties.get("model_dir")

        self.transform = get_val_transform()
        # DEBUG include this.
        # and properties.get("gpu_id") is not None
        device_str = "cuda:" + \
            str(properties.get("gpu_id")) if torch.cuda.is_available() else "cpu"
        print(f"Computing on {device_str}, using img_size = {IMG_SIZE}")
        self.device = torch.device(device_str)

        # Read model serialize/pt file
        serialized_file = self.manifest['model']['serializedFile']
        model_pt_path = os.path.join(model_dir, serialized_file)
        if not os.path.isfile(model_pt_path):
            raise RuntimeError("Missing the model.pt file")

        # ============ building network ... ============
        # TODO
        print(f"\nPreparing {MODEL_NAME}\n")
        print(f"Try loading from {model_pt_path}")
        if MODEL_NAME == "XcitClassifierHeadModelCuda":
            # state_dict = torch.load(model_pt_path)["state_dict"]
            # print(f"\nStatedict : {str(state_dict)}")
            print(f"\nInstantiating Model")
            from model import XcitClassifierHeadModelCuda
            self.model = XcitClassifierHeadModelCuda(arch="xcit_small_12_p16",
                                                     patch_size=16,
                                                     n_last_blocks=4,
                                                     avgpool_patchtokens=False,
                                                     backbone_weights_pth=None,
                                                     checkpoint_key=None,
                                                     device_id=device_str,
                                                     is_ddp=False)

            utils.restart_from_checkpoint(
                model_pt_path,
                #os.path.join(args.output_dir, "checkpoint.pth.tar"),
                state_dict=self.model
            )

                                            
            # self.model = self.model.load_state_dict(state_dict)
            print(f"\nModel Loaded {MODEL_NAME}\n")

        elif MODEL_NAME == "xcit_small_12_p16":
            self.model = torch.hub.load(
                'facebookresearch/xcit', MODEL_NAME, num_classes=0).to(self.device)
            # model.cuda()
            checkpoint_key = "teacher"  # guess u could load the student aswell
            load_pretrained_weights(self.model, pretrained_weights=model_pt_path,
                                    checkpoint_key=checkpoint_key, model_name=MODEL_NAME, patch_size=16)
        else:
            raise RuntimeError("No loading routine for given model")


        self.model.to(device=self.device)
        self.model.eval()
        self.initialized = True

    def preprocess(self, requests):
        """
        Transform raw input into model input data.
        :param batch: list of raw requests, should match batch size
        :return: list of preprocessed model input data
        """
        # Take the input data and make it inference ready
        images = []
        for idx, data in enumerate(requests):

            img_bytes = data.get("data")
            if img_bytes is None:
                img_bytes = data.get("body")

            img = bytes2pil(img_bytes)
            images.append(img)

        if images[0].size != (224, 224):
            warnings.warn(
                f"Img has to be resized, original size = {str(images[0].size)}")

        # already converts to tenso
        images = [self.transform(image) for image in images]
        images = torch.stack(images).to(self.device)

        return images

    # def postprocess(self, inference_output):
    #     """
    #     Return inference result.
    #     :param inference_output: list of inference output
    #     :return: list of predict results
    #     """
    #     # Take output from network and post-process to desired format
    #     postprocess_output = inference_output[0]
    #     return postprocess_output
