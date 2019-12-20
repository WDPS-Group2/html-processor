import nltk
import os

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tree import Tree

nltk.data.path.append(os.environ.get('PWD'))
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')


def structure_ne(ne_tree):
    ne = []
    for subtree in ne_tree:
        if type(subtree) == Tree:
            ne_label = subtree.label()
            ne_string = " ".join([token for token, pos in subtree.leaves()])
            ne.append((ne_string, ne_label))
    return ne


def nlp_preproc(text):
    tokens = word_tokenize(text)

    clean_tokens = tokens[:]
    sr = stopwords.words('english')
    for token in tokens:
        if token in sr:
            clean_tokens.remove(token)

    tagged_tokens = nltk.pos_tag(clean_tokens)
    ner_tagged_tokens = structure_ne(nltk.ne_chunk(tagged_tokens))

    return ner_tagged_tokens
