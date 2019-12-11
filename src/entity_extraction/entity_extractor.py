import nltk

from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')


def extract_entities(text):
    text = nltk.word_tokenize(text)
    text = nltk.pos_tag(text)
    text = [(word.lower(), pos) for word, pos in text]

    english_stopwords = stopwords.words("english")
    text = list(filter(lambda word_pos: word_pos[1].startswith("NN") and word_pos[0] not in english_stopwords, text))
    text = list(map(lambda word_pos: [word_pos[0], PorterStemmer().stem(word_pos[0]), word_pos[1]], text))

    return text