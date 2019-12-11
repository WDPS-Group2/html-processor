from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions
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
    def extract_entities_from_warc_spark_df(warc_df):
        extract_entities_udf = SparkExecutor.spark.udf.register("extract_entities_udf",
                                                                entity_extractor.extract_entities,
                                                                ArrayType(ArrayType(StringType())))

        entities_warc_df = warc_df.select("id", extract_entities_udf("html").alias("tagged_tokens"))
        exploded_df = entities_warc_df.withColumn("entity", functions.explode(entities_warc_df.tagged_tokens))
        lemmatized_df = exploded_df.selectExpr("id", "entity[0] as entity", "entity[1] as lemma", "entity[2] as POS")

        # TODO: Change group by from entity to the correct lemma version of all entities, when it is available
        group_data = lemmatized_df.groupBy("id", "entity")
        grouped_lemmatized_df = group_data.agg({"entity": "count"})\
            .withColumnRenamed("count(entity)", "N")\
            .orderBy("id", "N")

        return grouped_lemmatized_df

    @staticmethod
    def get_spark_dataframe_for_warc_filename(warc_filename):
        html_to_raw_udf = SparkExecutor.spark.udf.register("html_to_raw", html_processor.extract_raw_text_from_html)

        spark_schema = SparkExecutor.get_spark_schema()
        warc_pandas_df = warc_reader.convert_warc_to_dataframe(warc_filename)
        warc_df = SparkExecutor.spark.createDataFrame(warc_pandas_df, schema=spark_schema)

        print("Number of partitions for dataframe: %d" % (warc_df.rdd.getNumPartitions()))

        raw_warc_df = warc_df.select("id", "url", html_to_raw_udf("html").alias("html"))
        return raw_warc_df

