# requires pytorch
from transformers import AutoModel, AutoTokenizer
from sentence_transformers import SentenceTransformer
from transformers import ConvBertModel, ConvBertConfig
from tqdm import tqdm
from typing import List

import numpy as np
import torch

from mvfunctions.cloud.bigquery import BQParams
from mvfunctions.cloud.io import DFLoader
from mvmodels.text.bert import BertTextModel
from mvmodels.text.tfidf import TfidfTextModel
from scripts import test_utils

MAX_SEQUENCE_LENGTH = 220
BATCH_SIZE=2

from enum import Enum
class VecMethod(str, Enum):
    FIRST="first"
    MEAN="mean"
    SUM="sum"

def main():
    device = torch.device('cuda')
    # from mvfunctions.mvmlflow import MVMLFlow
    # import os
    # # create mlflow entry
    # with MVMLFlow(experiments_root_dir=os.getenv("EXPERIMENTS", "/tmp"),
    #               experiment_name="Pretrained_TextModels",
    #               run_name="cross-en-de-roberta-sentence-transformer",
    #               framework="pytorch") as mvmlflow:
    #     mvmlflow.set_tags({"model_name": 'T-Systems-onsite/cross-en-de-roberta-sentence-transformer'})
    #     test=123
    #     pass

    # import bert model
    #model = BertTextModel(model_name="dbmdz/convbert-base-german-europeana-cased")
    model = BertTextModel(model_name='T-Systems-onsite/cross-en-de-roberta-sentence-transformer', use_gpu=False)

    corpus: List[str] = get_example_corpus(use_json_file=True, debug_limit=1000)
    test_corpus = ["Bag Women"]

    # model returnes single vector for each sentence
    dataset_vecs = model.corpus2vecs(corpus)
    test_vecs = model.corpus2vecs(test_corpus)

    model_tfidf = TfidfTextModel()
    model_tfidf.fit(corpus)
    dataset_vecs_tfidf = model_tfidf.corpus2vecs(corpus)
    test_vecs_tfidf = model_tfidf.corpus2vecs(test_corpus)

    from sklearn.metrics import pairwise_distances
    dist_matrix = pairwise_distances(dataset_vecs, test_vecs, metric="cosine")
    matches_test_vec = np.argsort(dist_matrix[:, 0])[:10]
    for match_idx in matches_test_vec:
        print(corpus[match_idx])


def get_example_corpus(use_json_file=False,debug_limit=0) -> List[str]:
    import json
    if use_json_file:
        with open("data/bags_example_1000.json", "r") as fp:
            return json.load(fp)["corpus"][:debug_limit]
    else:
        # Laden der Daten
        src = "image-analysis-253512-dev.index_building_views.latest_awin_fashion24_bags_dataset_table_30c1f2d21b1149348262870a3b1b568c"
        text_col = "title"
        df = DFLoader(src=src, limit=debug_limit).df
        return df[text_col].to_list()

if __name__ == "__main__":
    main()