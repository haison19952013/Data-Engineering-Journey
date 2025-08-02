import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print("working_dir: " + working_dir)
    spark_conf = conf.get_spark_conf()
    spark = SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/dataframe-api/survey.csv")

    log.info("survey_df schema: ")
    survey_df.printSchema()

    count_df = survey_df \
        .filter(col("Age") < 40) \
        .groupBy("Country") \
        .agg(f.count("*").alias("count")) \
        .orderBy(col("count").desc(), col("Country"))

    log.info("count_df: ")
    count_df.show()

    spark.stop()
