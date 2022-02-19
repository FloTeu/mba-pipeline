from abc import ABC, abstractmethod
from typing import List
import numpy as np

class TextModelAbstract(ABC):
    """ Base Abstract Class for all text models
    """

    @abstractmethod
    def corpus2vecs(self, corpus: List[str]) -> np.ndarray:
        """ Takes a List of document (corpus) and transforms it to an multi dimensional vector with dimension (len(corpus), embedding_size)
        """
        pass

class MwTextModel(TextModelAbstract):
    """ Base Class for all text models
        TODO: Define some basic functionalities inside this class e.g. set stop_words in init or language
    """
    pass
