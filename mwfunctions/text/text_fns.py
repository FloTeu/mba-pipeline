import os
import nltk
from collections import OrderedDict
import numpy as np
import spacy
from rake_nltk import Metric, Rake
from mwfunctions.pydantic import TextLanguage

from typing import Optional, List, Set, Dict
from nltk.stem.snowball import SnowballStemmer
#from nltk.corpus import stopwords
from mwfunctions.pydantic.base_classes import EnumBase, Marketplace


def textLanguage2SpacyDefaultPackage(language: TextLanguage) -> str:
    if language == TextLanguage.GERMAN:
        return "de_core_news_sm"
    elif language == TextLanguage.ENGLISH:
        return "en_core_web_sm"
    elif language == TextLanguage.SPANISH:
        return "es_core_news_sm"
    elif language == TextLanguage.FRENCH:
        return "fr_core_news_sm"
    elif language == TextLanguage.ITALIAN:
        return "it_core_news_sm"
    elif language == TextLanguage.JAPANESE:
        return "ja_core_news_sm"
    else:
        raise NotImplementedError

def download_language_package(language_package: str):
    os.system(f"python3 -m spacy download {language_package}")

def download_default_language_package(language: TextLanguage):
    download_language_package(textLanguage2SpacyDefaultPackage(language))

class TextRank4Keyword():
    """Extract keywords from text
        Tutorial: https://towardsdatascience.com/textrank-for-keyword-extraction-by-python-c0bae21bcec0
    """

    def get_nlp_obj(self):
        try:
            return spacy.load(textLanguage2SpacyDefaultPackage(self.language))
        except OSError:
            download_default_language_package(self.language)
            return spacy.load(textLanguage2SpacyDefaultPackage(self.language))

    def __init__(self, language: TextLanguage=TextLanguage.ENGLISH):
        self.d = 0.85  # damping coefficient, usually is .85
        self.min_diff = 1e-5  # convergence threshold
        self.steps = 10  # iteration steps
        self.node_weight = None  # save keywords and its weight
        assert language in TextLanguage.to_list(), f"Language need to be one of {TextLanguage.to_list()} but is {TextLanguage.to_list()}"
        self.language = language

        self.nlp = self.get_nlp_obj()

    def set_stopwords(self, stopwords):
        """Set stop words"""
        for word in language2StopWords(text_language=self.language).union(set(stopwords)):
            lexeme = self.nlp.vocab[word]
            lexeme.is_stop = True

    def sentence_segment(self, doc, candidate_pos, lower):
        """Store those words only in cadidate_pos"""
        sentences = []
        for sent in doc.sents:
            selected_words = []
            for token in sent:
                # Store words only with cadidate POS tag
                if token.pos_ in candidate_pos and token.is_stop is False:
                    if lower is True:
                        selected_words.append(token.text.lower())
                    else:
                        selected_words.append(token.text)
            sentences.append(selected_words)
        return sentences

    def get_vocab(self, sentences):
        """Get all tokens"""
        vocab = OrderedDict()
        i = 0
        for sentence in sentences:
            for word in sentence:
                if word not in vocab:
                    vocab[word] = i
                    i += 1
        return vocab

    def get_token_pairs(self, window_size, sentences):
        """Build token_pairs from windows in sentences"""
        token_pairs = list()
        for sentence in sentences:
            for i, word in enumerate(sentence):
                for j in range(i + 1, i + window_size):
                    if j >= len(sentence):
                        break
                    pair = (word, sentence[j])
                    if pair not in token_pairs:
                        token_pairs.append(pair)
        return token_pairs

    def symmetrize(self, a):
        return a + a.T - np.diag(a.diagonal())

    def get_matrix(self, vocab, token_pairs):
        """Get normalized matrix"""
        # Build matrix
        vocab_size = len(vocab)
        g = np.zeros((vocab_size, vocab_size), dtype='float')
        for word1, word2 in token_pairs:
            i, j = vocab[word1], vocab[word2]
            g[i][j] = 1

        # Get Symmeric matrix
        g = self.symmetrize(g)

        # Normalize matrix by column
        norm = np.sum(g, axis=0)
        g_norm = np.divide(g, norm, where=norm != 0)  # this is ignore the 0 element in norm

        return g_norm

    def get_keywords(self, number=10, verbose=False):
        """Print top number keywords"""
        node_weight = OrderedDict(sorted(self.node_weight.items(), key=lambda t: t[1], reverse=True))
        keywords = []
        for i, (key, value) in enumerate(node_weight.items()):
            if verbose:
                print(key + ' - ' + str(value))
            keywords.append(key)
            if i > number:
                return keywords
        return keywords

    def get_unsorted_keywords(self, text,
                              candidate_pos=['NOUN', 'PROPN'],
                              lower=False, stopwords=list()):
        # Set stop words
        self.set_stopwords(stopwords)

        # Pare text by spaCy
        doc = self.nlp(text)

        # get connected words
        if lower:
            entities = list([ent.text.lower() for ent in doc.ents])
        else:
            entities = list([ent.text for ent in doc.ents])

        # filter stopwords from entities (only those where each word is a stopword)
        entities = [ent for ent in entities if all(all(stopword != ent_word for ent_word in ent.split(" ")) for stopword in stopwords)]

        # Filter sentences
        sentences = self.sentence_segment(doc, candidate_pos, lower)  # list of list of words

        # Build vocabulary
        vocab = self.get_vocab(sentences)

        longtail_keywords = self.get_rake_longtail_keywords(text, stopwords=stopwords)

        # drop duplicates
        keywords = list(dict.fromkeys(list(vocab) + entities + longtail_keywords))
        # return keywords
        return keywords

    def get_rake_longtail_keywords(self, text, stopwords=list()):
        language_name = "english"
        if self.language == "de":
            language_name = "german"

        try:
            r = Rake(language=language_name, min_length=2, max_length=5, stopwords=stopwords)
            r.extract_keywords_from_text(text)
        except Exception as e:
            nltk.download("punkt")
            r = Rake(language=language_name, min_length=2, max_length=5, stopwords=stopwords)
            r.extract_keywords_from_text(text)
        return r.get_ranked_phrases()

    def is_meaningful_keyword(self, text, keyword_to_poc):
        important_kinds = ['NOUN', 'PROPN', "VERB"]

        for token_text in text.split(" "):
            try:
                if keyword_to_poc[token_text] in important_kinds:
                    return True
            except:
                pass
        return False

    def analyze(self, text,
                candidate_pos=['NOUN', 'PROPN'],
                window_size=4, lower=False, stopwords=list()):
        """Main function to analyze text"""

        # Set stop words
        self.set_stopwords(stopwords)

        # Pare text by spaCy
        doc = self.nlp(text)

        # Filter sentences
        sentences = self.sentence_segment(doc, candidate_pos, lower)  # list of list of words

        # Build vocabulary
        vocab = self.get_vocab(sentences)

        # Get token_pairs from windows
        token_pairs = self.get_token_pairs(window_size, sentences)

        # Get normalized matrix
        g = self.get_matrix(vocab, token_pairs)

        # Initionlization for weight(pagerank value)
        pr = np.array([1] * len(vocab))

        # Iteration
        previous_pr = 0
        for epoch in range(self.steps):
            pr = (1 - self.d) + self.d * np.dot(g, pr)
            if abs(previous_pr - sum(pr)) < self.min_diff:
                break
            else:
                previous_pr = sum(pr)

        # Get weight for each node
        node_weight = dict()
        for word, index in vocab.items():
            node_weight[word] = pr[index]

        self.node_weight = node_weight

Language2TR4W_DICT_CACHE: Dict[TextLanguage, TextRank4Keyword] = {}

def get_tr4w_obj(language: TextLanguage):
    if language in Language2TR4W_DICT_CACHE:
        tr4w = Language2TR4W_DICT_CACHE[language]
    else:
        tr4w = TextRank4Keyword(language=language)
        Language2TR4W_DICT_CACHE[language] = tr4w
    return tr4w

cwd = os.getcwd()

nltk.data.path.append(os.path.join(cwd,"nltk_data"))
nltk.data.path.append("nltk_data")
nltk.data.path.append(".nltk_data")
nltk.data.path.append(".")


class StemmerLanguage(str, EnumBase):
    # contains names to init stemmer from nltk library. Is different to TextLanguage, because englisch products in german MBA market are stemmed with german stemmer, but TextLanguage is ENGLISH
    GERMAN="german"
    ENGLISH="english"
    ITALIAN="italian"
    SPANISH="spanish"
    FRENCH="french"
    #JAPANESE="japanese" TODO: how to handle japanese stem operation?

def marketplace2StemmerLanguage(marketplace: Marketplace):
    if marketplace == Marketplace.DE:
        return StemmerLanguage.GERMAN
    elif marketplace == Marketplace.COM:
        return StemmerLanguage.ENGLISH
    else:
        raise NotImplementedError


def language2StopWords(stemmer_language: StemmerLanguage=None, text_language: TextLanguage=None) -> Set[str]:
    # import within function to reduce startup time
    if stemmer_language == StemmerLanguage.GERMAN or text_language == TextLanguage.GERMAN:
        from spacy.lang.de.stop_words import STOP_WORDS as STOP_WORDS_DE
        return STOP_WORDS_DE
    elif stemmer_language == StemmerLanguage.ENGLISH or text_language == TextLanguage.ENGLISH:
        from spacy.lang.en.stop_words import STOP_WORDS
        return STOP_WORDS
    elif stemmer_language == StemmerLanguage.ITALIAN or text_language == TextLanguage.ITALIAN:
        from spacy.lang.it.stop_words import STOP_WORDS as STOP_WORDS_IT
        return STOP_WORDS_IT
    elif stemmer_language == StemmerLanguage.SPANISH or text_language == TextLanguage.SPANISH:
        from spacy.lang.es.stop_words import STOP_WORDS as STOP_WORDS_ES
        return STOP_WORDS_ES
    elif stemmer_language == StemmerLanguage.FRENCH or text_language == TextLanguage.FRENCH:
        from spacy.lang.fr.stop_words import STOP_WORDS as STOP_WORDS_FR
        return STOP_WORDS_FR
    elif text_language == TextLanguage.JAPANESE:
        from spacy.lang.ja.stop_words import STOP_WORDS as STOP_WORDS_JA
        return STOP_WORDS_JA
    else:
        raise NotImplementedError

# this function requires nltk_data
# TODO: Does it also requires to download stuff first? dont now where..
def get_stem_keywords_language(keywords: List[str], language: Optional[StemmerLanguage]=None, marketplace: Optional[Marketplace]=None):
    assert language!=None or marketplace!=None, "Either language or marketplace must be set"
    stem_keywords = []
    language: StemmerLanguage = language if language else marketplace2StemmerLanguage(marketplace)
    stop_words = language2StopWords(language)
    keywords_filtered = [w for w in keywords if not w in stop_words]
    snowball_stemmer = SnowballStemmer(language)

    for keyword in keywords_filtered:
        stem_keywords.append(snowball_stemmer.stem(keyword))
    return stem_keywords

"""
### Constants
"""

# keywords everybody uses and therefore dont have an impact for further anylsis (are not meaningful)
KEYWORDS_TO_REMOVE_DE = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "Geschenk", "Geschenkidee", "Design",
                              "Weihnachten", "Frau",
                              "Geburtstag", "Freunde", "Sohn", "Tochter", "Vater", "Geburtstagsgeschenk", "Herren",
                              "Frauen", "Mutter", "Schwester", "Bruder", "Kinder",
                              "Spruch", "Fans", "Party", "Geburtstagsparty", "Familie", "Opa", "Oma", "Liebhaber",
                              "Freundin", "Freund", "Jungen", "Mädchen", "Outfit",
                              "Motiv", "Damen", "Mann", "Papa", "Mama", "Onkel", "Tante", "Nichte", "Neffe", "Jungs",
                              "gift", "Marke", "Kind", "Anlass", "Jubiläum"
    , "Überraschung"]
KEYWORDS_TO_REMOVE_EN = ["T-Shirt", "tshirt", "Shirt", "shirt", "T-shirt", "gift", "Brand", "family", "children",
                              "friends", "sister", "brother",
                              "childreen", "present", "boys", "girls"]

KEYWORDS_TO_REMOVE_MARKETPLACE_DICT = {"de": KEYWORDS_TO_REMOVE_DE, "com": KEYWORDS_TO_REMOVE_EN}
TextLanguage2KeywordsToRemove_dict = {TextLanguage.ENGLISH: KEYWORDS_TO_REMOVE_EN, TextLanguage.GERMAN: KEYWORDS_TO_REMOVE_DE,
                                      TextLanguage.FRENCH: [], TextLanguage.ITALIAN: [], TextLanguage.SPANISH: [], TextLanguage.JAPANESE: []}
