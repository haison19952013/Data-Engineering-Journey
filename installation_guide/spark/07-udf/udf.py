import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StringType

import my_util.gender_util as gender_util
import my_util.no_employee_util as no_employee_util
import util.config as conf
from util.logger import Log4j

import re

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StructType, StructField


def parse_employees(val):
    if val is None:
        return (None, None)
    if "More than" in val:
        num = int(re.search(r"\d+", val).group())
        return (num + 1, None)  # (lower bound, open upper bound)
    elif "-" in val:
        lo, hi = val.split("-")
        return (int(lo), int(hi))
    else:
        try:
            return (int(val), int(val))
        except:
            return (None, None)

schema = StructType([
    StructField("low", IntegerType(), True),
    StructField("high", IntegerType(), True)
])
parse_employees_udf = udf(parse_employees, returnType=schema)

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

    survey_df.withColumn("no_employees", no_employee_util.parse_employees_udf("no_employees")) \
        .select("Age", "Gender", "Country", "state", "no_employees.low", "no_employees.high") \
        .filter("no_employees.low >= 500") \
        .show()

    spark.stop()
