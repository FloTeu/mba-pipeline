import numpy as np
import torch

from torch.nn.modules.module import Module
from sentence_transformers import SentenceTransformer
from typing import List, Optional
from enum import Enum
from tqdm import tqdm

from mwfunctions.mlmodels.text import MwTextModel
from sagemaker.huggingface import HuggingFacePredictor, HuggingFaceModel
from sagemaker.serializers import JSONSerializer
from sagemaker.deserializers import JSONDeserializer

MAX_SEQUENCE_LENGTH = 220
BATCH_SIZE = 2

class BertPretrainedModelNames(str, Enum):
    CONV_BERT_DE = "dbmdz/convbert-base-german-europeana-cased"
    ROBERTA_DE_EN = 'T-Systems-onsite/cross-en-de-roberta-sentence-transformer'

# class BertAWSEndpointName(str, Enum):
#     ROBERTA_DE_EN = 'roberta-serverless'

class BertPretrainedModelType(str, Enum):
    TRANSFORMER="transformer" # lib transformers
    SENTENCE_TRANSFORMER="sentence_transformer" # lib sentence_transformers

class VecMethod(str, Enum):
    FIRST="first"
    MEAN="mean"
    SUM="sum"

class BertTextModel(MwTextModel):
    """ Note: Currently only works with Pytorch
    """
    def __init__(self, mlflow_run_id: str=None, model_name: BertPretrainedModelNames=None, endpoint_name: str=None, use_gpu=True, batch_size=None):
        assert mlflow_run_id or model_name or endpoint_name, "Either mlflow_run_id or model_name or endpoint_name must be provided"
        # if not model_name:
        #     mvmlflow = MVMLFlow(run_id=mlflow_run_id)
        #     assert "model_name" in mvmlflow.tags, "Mlflow Model must contain tag 'model_name'"
        #     model_name = mvmlflow.tags["model_name"]

        self.device = torch.device('cuda') if use_gpu else torch.device('cpu')
        self.batch_size = batch_size if batch_size else BATCH_SIZE
        self.endpoint_name = endpoint_name
        self.model_name = model_name if not self.endpoint_name else None
        self.model_type: BertPretrainedModelType = self.model_name2model_type(model_name) if not self.endpoint_name else None
        # TODO: Eventuell macht es hier Sinn eine eigen Klasse zu bauen, die z.B. predict als standard Funktion hat und mit unterschiedlichen Model Klassen umgehen kann
        if endpoint_name:
            self.predictor = HuggingFacePredictor(endpoint_name=self.endpoint_name,
                             serializer=JSONSerializer(),
                             deserializer=JSONDeserializer())
        else:
            self.model: Module = self.model_name2model(model_name, self.model_type).to(self.device)
        self.tokenizer = self.model_name2tokenizer(model_name) if not self.endpoint_name else None


    @staticmethod
    def model_name2model_type(model_name: BertPretrainedModelNames) -> BertPretrainedModelType:
        if model_name == BertPretrainedModelNames.CONV_BERT_DE:
            return BertPretrainedModelType.TRANSFORMER
        elif model_name == BertPretrainedModelNames.ROBERTA_DE_EN:
            return BertPretrainedModelType.SENTENCE_TRANSFORMER

    @staticmethod
    def model_name2model(model_name: BertPretrainedModelNames, model_type: BertPretrainedModelType) -> Module:
        from transformers import AutoModel, AutoTokenizer
        if model_type == BertPretrainedModelType.TRANSFORMER:
            return AutoModel.from_pretrained(model_name)
        elif model_type == BertPretrainedModelType.SENTENCE_TRANSFORMER:
            return SentenceTransformer(model_name)

    def model_name2tokenizer(self, model_name: BertPretrainedModelNames):
        from transformers import AutoModel, AutoTokenizer
        # TODO: Does BertPretrainedModelType.SENTENCE_TRANSFORMER never contains a tokenizer?
        return AutoTokenizer.from_pretrained(model_name) if self.model_type == BertPretrainedModelType.TRANSFORMER else None

    def corpus2vecs(self, corpus: List[str]) -> np.ndarray:
        """ Takes a corpus (i.e list of documents/texts) into a vector with dim (len(corpus), )
        """
        if self.endpoint_name:
            response = self.predictor.predict({"inputs": corpus})
            response_np = np.array(response)
            return response_np
        elif self.model_type == BertPretrainedModelType.SENTENCE_TRANSFORMER:
            return self.model.encode(corpus)
        elif self.model_type == BertPretrainedModelType.TRANSFORMER:
            return corpus2vecs_bert_transformer(corpus, self.model, self.tokenizer, device=self.device, method=VecMethod.FIRST, include_class_token=True)
        else:
            raise NotImplementedError

def corpus2vecs_bert_transformer(corpus: List[str], model, tokenizer, device=None, method: VecMethod=VecMethod.FIRST, include_class_token=False):
    device = device if device else torch.device('cuda')
    text_token_ids, token_lengths = tokenize_and_pad(corpus, MAX_SEQUENCE_LENGTH, tokenizer)
    pred_vecs: List[np.ndarray] = []
    text_token_ids_dataset = torch.utils.data.TensorDataset(torch.tensor(text_token_ids, dtype=torch.long))
    # transform dataset to batches
    data_loader = torch.utils.data.DataLoader(text_token_ids_dataset, batch_size=BATCH_SIZE, shuffle=False)
    for i, (x_batch,) in enumerate(tqdm(data_loader)):
        pred_batch = model(x_batch.to(device), attention_mask=(x_batch > 0).to(device))
        # Extend first class token vector
        # pred.to_tuple()[0] => (BATCH_SIZE, MAX_SEQUENCE_LENGTH, 768)
        for j, pred in enumerate(pred_batch.to_tuple()[0]):
            token_length_index = BATCH_SIZE * i + j
            pred_vecs.append(apply_vecs_method(method, pred, token_lengths[token_length_index], include_class_token=include_class_token))

    return np.array(pred_vecs)

def apply_vecs_method(method, pred: np.ndarray, token_length, include_class_token=False) -> np.ndarray:
    """ Takes a VecMethod and model prediction result for one sentence and returns a one dimensional vector.

        Note: pred has dim (MAX_SEQUENCE_LENGTH, 768)
    """
    from sklearn.preprocessing import normalize

    if method in [VecMethod.MEAN, VecMethod.SUM]:
        np_array = pred[0 if include_class_token else 1:token_length + 1].detach().cpu().squeeze().numpy()  # ensure dim (token_length, 768)
        if len(np_array.shape) == 1:
            np_array = np_array.reshape(1, -1)
        for i, np_array_row in enumerate(np_array):
            np_array[i] = normalize(np_array_row[:, np.newaxis], axis=0).ravel()

    def return_normalized(vec, norm="l2"):
        return normalize(vec.reshape(1,-1), norm=norm)[0]

    if method == VecMethod.FIRST:
        return return_normalized(pred[0].detach().cpu().squeeze().numpy())
    elif method == VecMethod.MEAN:
        return return_normalized(np_array.mean(axis=0))
    elif method == VecMethod.SUM:
        return return_normalized(np_array.sum(axis=0))
    else:
        raise NotImplementedError

def tokenize_and_pad(corpus: List[str], ensure_length_int: int, tokenizer, do_padding=True) -> np.ndarray:
    """ Tokenizes text + convert tokens to ids.
        Additionally first token is appended which is class token + end token as well as as much padding 0 values as ensure_length_int
        Returns a list of ensure_length_int integer values.
    """
    # Code von https://www.kaggle.com/abhishek/pytorch-bert-inference
    # Kp ob das hier richtig gemacht wird
    ensure_length_int -= 2
    all_tokens = []
    token_lengths = []
    longer = 0
    for text in tqdm(corpus):
        tokens_a = tokenizer.tokenize(text)
        if len(tokens_a) > ensure_length_int:
            tokens_a = tokens_a[:ensure_length_int]
            longer += 1
        token_lengths.append(len(tokens_a))
        one_token = tokenizer.convert_tokens_to_ids(["[CLS]"] + tokens_a + ["[SEP]"]) + [0] * (
                    ensure_length_int - len(tokens_a))
        all_tokens.append(one_token)
    return np.array(all_tokens), token_lengths
