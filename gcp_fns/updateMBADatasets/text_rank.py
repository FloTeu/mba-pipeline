from collections import OrderedDict
import numpy as np
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.lang.de.stop_words import STOP_WORDS as STOP_WORDS_DE
from rake_nltk import Metric, Rake


class TextRank4Keyword():
    """Extract keywords from text
        Tutorial: https://towardsdatascience.com/textrank-for-keyword-extraction-by-python-c0bae21bcec0
    """
    
    def __init__(self, language="en"):
        self.d = 0.85 # damping coefficient, usually is .85
        self.min_diff = 1e-5 # convergence threshold
        self.steps = 10 # iteration steps
        self.node_weight = None # save keywords and its weight
        assert language in ["de", "en"], "Language need to be either en or de"
        self.language = language
        if self.language == "en":
            self.nlp = spacy.load('en_core_web_sm')
            self.nlp_de = spacy.load('de_core_news_sm')
        elif self.language == "de":
            self.nlp = spacy.load('de_core_news_sm')
            self.nlp_en = spacy.load('en_core_web_sm')
    
    def set_stopwords(self, stopwords):  
        """Set stop words"""
        if self.language == "en":
            for word in STOP_WORDS.union(set(stopwords)):
                lexeme = self.nlp.vocab[word]
                lexeme.is_stop = True
        elif self.language == "de":
            for word in STOP_WORDS_DE.union(set(stopwords)):
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
                for j in range(i+1, i+window_size):
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
        g_norm = np.divide(g, norm, where=norm!=0) # this is ignore the 0 element in norm
        
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
        
        # Filter sentences
        sentences = self.sentence_segment(doc, candidate_pos, lower) # list of list of words
        
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
        sentences = self.sentence_segment(doc, candidate_pos, lower) # list of list of words
        
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
            pr = (1-self.d) + self.d * np.dot(g, pr)
            if abs(previous_pr - sum(pr))  < self.min_diff:
                break
            else:
                previous_pr = sum(pr)

        # Get weight for each node
        node_weight = dict()
        for word, index in vocab.items():
            node_weight[word] = pr[index]
        
        self.node_weight = node_weight