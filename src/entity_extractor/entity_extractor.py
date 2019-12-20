from preprocessing.html_converter import html2text
from preprocessing.nlp_preproc_spark import nlp_preproc
from entity_linking.elasticsearch import search
from entity_linking.sparql import query_abstract
from sklearn.feature_extraction.text import TfidfVectorizer

import nltk

KEYNAME = "WARC-TREC-ID"

entity_dict = {}
abstract_dict = {}
vectorizer = TfidfVectorizer()
print("Hopa")


def find_key(payload):
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(":")[1]
            return key
    return ""


def cosine_sim(text1, text2):
        tfidf = vectorizer.fit_transform([text1, text2])
        return ((tfidf * tfidf.T).A)[0, 1]


def ner_tagged_tokens(record):
    _, payload = record
    key = find_key(payload)
    if key != "":
        text = html2text(payload)
        tagged_tokens = nlp_preproc(text)
        for token in tagged_tokens:
            yield key, text, token[0]


def search_candidate(token, elasticsearch_host):
    if entity_dict.__contains__(token):
        entities = entity_dict[token]
    else:
        entities = search(elasticsearch_host, token).items()
        entity_dict[token] = entities
    return entities


def query_candidate_abstract(entity, sparql_host):
    if abstract_dict.__contains__(entity):
        abstract = abstract_dict[entity]
    else:
        abstract = query_abstract(sparql_host, entity)
        abstract_dict[entity] = abstract
    return abstract


def candidate_entity_generation(record, elasticsearch_host, sparql_host):
    key, text, token = record
    entities = search_candidate(token, elasticsearch_host)
    entities_list = []
    for entity, labels in entities:
        abstract = query_candidate_abstract(entity, sparql_host)
        if abstract is not None:
            entities_list.append([entity, abstract])

    if entities_list:
        yield key, text, token, entities_list


def find_abstract_object(abstract):
    abstract_token = nltk.word_tokenize(abstract)
    abstract_pos_tag = nltk.pos_tag(abstract_token)
    obj = ""
    for token in abstract_pos_tag:
        if token[1].startswith("VB"):
            break
        else:
            obj = obj + token[0] + " "
    return obj


def candidate_entity_ranking(record):
    key, text, token, entities = record
    score_max = 0
    entity_score_max = ""
    for entity in entities:
        abstract = entity[1]
        score = 0
        abstract_object = find_abstract_object(abstract)
        if abstract_object != "":
            if cosine_sim(abstract_object, token) < 0.1:
                continue
            else:
                score = score + cosine_sim(abstract_object, token)
        score = cosine_sim(text, abstract)
        if score > score_max:
            score_max = score
            entity_score_max = entity[0]
    if score_max != 0:
        return_val = key + '\t' + token + '\t' + entity_score_max
        print("Ranked candidates!")
        yield return_val