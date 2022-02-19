from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.utils.validation import check_is_fitted
from sklearn.exceptions import NotFittedError
from typing import List

import numpy as np

from mwfunctions.mlmodels.text import MwTextModel

class TfidfTextModel(MwTextModel):
    """ Note: Currently only works with Pytorch
    """
    def __init__(self):
        self.model = TfidfVectorizer()
        pass

    def fit(self, corpus: List[str]):
        self.model = self.model.fit(corpus)

    def corpus2vecs(self, corpus: List[str]) -> np.ndarray:
        if not self.is_fitted():
            raise ValueError("Model is not fitted with corpus data. Call fit()")
        return self.model.transform(corpus)

    def is_fitted(self):
        try:
            check_is_fitted(self.model)
            return True
        except NotFittedError:
            return False
