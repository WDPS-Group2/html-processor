import argparse
import os
import time
import sys

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')

from html_handler import html_processor
from warc import warc_checker, warc_reader
from entity_extraction import entity_extractor
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def is_valid_file(parser, filename):
    if not os.path.exists(filename):
        parser.error("The file %s does not exist!" % filename)
    elif not warc_checker.is_file_valid_warc_file(filename):
        parser.error("The file %s is not a .gz archive" % filename)
    return filename


def parse_arguments():
    parser = argparse.ArgumentParser(description='Warc processor')
    parser.add_argument("-f", "--filename", dest="filename",
                        required=True, help="warc file archive",
                        metavar="FILE", type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()
    return args


def get_spark_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("html", StringType(), True)])


def get_spark_dataframe_for_warc_filename(warc_filename):
    spark = SparkSession.builder.appName('htmlProcessor').getOrCreate()
    sc = spark.sparkContext
    print("Default parallelism: %d" % sc.defaultParallelism)

    html_to_raw_udf = spark.udf.register("html_to_raw", html_processor.extract_raw_text_from_html)

    spark_schema = get_spark_schema()
    warc_pandas_df = warc_reader.convert_warc_to_dataframe(warc_filename)
    warc_df = spark.createDataFrame(warc_pandas_df, schema=spark_schema)

    print("Number of partitions for dataframe: %d" % (warc_df.rdd.getNumPartitions()))

    raw_warc_df = warc_df.select("id", "url", html_to_raw_udf("html").alias("html"))
    return raw_warc_df


start_time = time.time()

args = parse_arguments()
filename = args.filename


warc_df = get_spark_dataframe_for_warc_filename(filename)
warc_df.show()
# warc_df.select('html').foreach()

duration = time.time() - start_time
print("Execution duration: %.2fs" % duration)

