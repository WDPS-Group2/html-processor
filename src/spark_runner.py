import sys
import time

from pyspark import SparkContext
from entity_extractor import entity_extractor

start_time = time.time()

print("Generating Spark Context")
sc = SparkContext("yarn", "spark-runner-wdps1902")

INFILE = sys.argv[1]
OUTFILE = sys.argv[2]
ELASTICSEARCH_HOST = sys.argv[3]
SPARQL_HOST = sys.argv[4]

rdd = sc.newAPIHadoopFile(INFILE,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.LongWritable",
                          "org.apache.hadoop.io.Text",
                          conf={"textinputformat.record.delimiter": "WARC/1.0"})\
        .flatMap(entity_extractor.ner_tagged_tokens)\
        .flatMap(lambda record: entity_extractor.candidate_entity_generation(record, ELASTICSEARCH_HOST, SPARQL_HOST))\
        .flatMap(entity_extractor.candidate_entity_ranking)


print("Parallelism: %d" % sc.defaultParallelism)
print("Nr partitions: %d" % rdd.getNumPartitions())

rdd.saveAsTextFile(OUTFILE)

duration = time.time() - start_time
print("Total duration: %.2f" % duration)
