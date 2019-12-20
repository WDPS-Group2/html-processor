import sys

from pyspark import SparkContext
from html2text import html2text
from nlp_preproc_spark import nlp_preproc
from elasticsearch import search
from sparql import query_abstract
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk
import time

start_time = time.time()

print("Generating Spark Context")
sc = SparkContext("yarn", "spark-runner-wdps1902")

KEYNAME = "WARC-TREC-ID"
INFILE = sys.argv[1]
OUTFILE = sys.argv[2]
ELASTICSEARCH = sys.argv[3]
SPARQL = sys.argv[4]

entity_dict = {}
abstract_dict = {}
vectorizer = TfidfVectorizer()


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


def search_candidate(token):
    if entity_dict.__contains__(token):
        entities = entity_dict[token]
    else:
        entities = search(ELASTICSEARCH, token).items()
        entity_dict[token] = entities
    return entities


def query_candidate_abstract(entity):
    if abstract_dict.__contains__(entity):
        abstract = abstract_dict[entity]
    else:
        abstract = query_abstract(SPARQL, entity)
        abstract_dict[entity] = abstract
    return abstract


def candidate_entity_generation(record):
    key, text, token = record
    print("Generating candidate entities for record: %s" % token)
    entities = search_candidate(token)
    entities_list = []
    print("Got %d candidates for entity: %s" % (len(entities), token))
    for entity, labels in entities:
        abstract = query_candidate_abstract(entity)
        print("Got abstract for entity: %s: %s" % (entity, abstract))
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
    print("Ranking candidates")
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
        print("Entity ranking: %s" % return_val)
        yield return_val


rdd = sc.newAPIHadoopFile(INFILE,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.LongWritable",
                          "org.apache.hadoop.io.Text",
                          conf={"textinputformat.record.delimiter": "WARC/1.0"})\
        .flatMap(ner_tagged_tokens)\
        .flatMap(candidate_entity_generation)\
        .flatMap(candidate_entity_ranking)


print("Parallelism: %d" % sc.defaultParallelism)
print("Nr partitions: %d" % rdd.getNumPartitions())

rdd.saveAsTextFile(OUTFILE)
duration = time.time() - start_time
print("Total duration: %.2f" % duration)
