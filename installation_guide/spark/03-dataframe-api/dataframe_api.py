import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, when, lower


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

    # Question 1: Count the number of survey participants from each country where age is less than 40.
    count_df = survey_df \
        .filter(col("Age") < 40) \
        .groupBy("Country") \
        .agg(f.count("*").alias("count")) \
        .orderBy(col("count").desc(), col("Country"))

    log.info("count_df: ")
    count_df.show()
    
    # Question 2: Count the number of male and female survey participants from each country. For male, Gender column can have values `male`, `man`, `m`. For female, Gender column can have values `female`, `woman`, `f`. Remind that those values are not case sensitive.
    gender_count_df = survey_df \
        .filter(col("Gender").isNotNull()) \
        .groupBy("Country") \
        .agg(
            f.count(when(lower(col("Gender")).isin("male", "man", "m"), True)).alias("num_male"),
            f.count(when(lower(col("Gender")).isin("female", "woman", "f"), True)).alias("num_female")
        ) \
        .orderBy(col("Country"))

    log.info("gender_count_df: ")
    gender_count_df.show()

    spark.stop()
