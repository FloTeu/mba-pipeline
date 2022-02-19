from sklearn.feature_extraction.text import TfidfVectorizer
from typing import List
import numpy as np

def get_trained_tfidf_vectorizer(corpus):
    vectorizer = TfidfVectorizer()
    return vectorizer.fit(corpus)


def tdidf_text_list2vecs(text_list: List[str]) -> np.ndarray:
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(text_list)
    return X