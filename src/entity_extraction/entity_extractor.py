import nltk

nltk.download('average_perceptro_tagger')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')


def extract_entities(text):
    text = nltk.word_tokenize(text)
    text = nltk.pos_tag(text)
    return text