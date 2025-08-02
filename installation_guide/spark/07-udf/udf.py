import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType

import gender_util.gender_util as gender_util
import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    log = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/data/udf/survey.csv")

    log.info("survey_df schema:")
    survey_df.printSchema()

    log.info("survey_df:")
    survey_df.show()

    log.info("Catalog Entry:")
    for r in spark.catalog.listFunctions():
        if "parse_gender" in r.name:
            log.info(r)

    survey_df.withColumn("Gender", gender_util.parse_gender_udf("Gender")) \
        .select("Age", "Gender", "Country", "state", "no_employees") \
        .show()

    spark.udf.register("parse_gender_udf", gender_util.parse_gender, StringType())
    log.info("Catalog Entry:")
    for r in spark.catalog.listFunctions():
        if "parse_gender" in r.name:
            log.info(r)

    survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)")) \
        .select("Age", "Gender", "Country", "state", "no_employees") \
        .show()

    spark.stop()
