from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from html_handler import html_processor
from warc import warc_reader
from entity_extraction import entity_extractor


class SparkExecutor:

    APPLICATION_NAME = "warcExtractor"
    spark = SparkSession.builder.appName(APPLICATION_NAME).getOrCreate()
    sc = spark.sparkContext

    @staticmethod
    def get_spark_schema():
        return StructType([
            StructField("id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("html", StringType(), True)])

    @staticmethod
    def extract_entities_from_raw_text(html_raw_text):
        entities = entity_extractor.extract_entities(html_raw_text)
        return entities

    @staticmethod
    def extract_entities_from_warc_spark_df(warc_df):
        extract_entities_udf = SparkExecutor.spark.udf.register("extract_entities_udf",
                                                                SparkExecutor.extract_entities_from_raw_text,
                                                                ArrayType(ArrayType(StringType())))

        entities_warc_df = warc_df.select("id", extract_entities_udf("html").alias("tagged_tokens"))
        exploded_df = entities_warc_df.withColumn("entity", explode(entities_warc_df.tagged_tokens))
        lemmatized_df = exploded_df.selectExpr("id", "entity[0] as entity", "entity[1] as lemma", "entity[2] as POS")
        return lemmatized_df

    @staticmethod
    def get_spark_dataframe_for_warc_filename(warc_filename):
        html_to_raw_udf = SparkExecutor.spark.udf.register("html_to_raw", html_processor.extract_raw_text_from_html)

        spark_schema = SparkExecutor.get_spark_schema()
        warc_pandas_df = warc_reader.convert_warc_to_dataframe(warc_filename)
        warc_df = SparkExecutor.spark.createDataFrame(warc_pandas_df, schema=spark_schema)

        print("Number of partitions for dataframe: %d" % (warc_df.rdd.getNumPartitions()))

        raw_warc_df = warc_df.select("id", "url", html_to_raw_udf("html").alias("html"))
        return raw_warc_df

