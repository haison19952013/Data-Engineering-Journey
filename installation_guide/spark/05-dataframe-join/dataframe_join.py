import os

from pyspark.sql import SparkSession

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    flight_time_df1 = spark.read.json("/data/dataframe-join/d1/")
    flight_time_df2 = spark.read.json("/data/dataframe-join/d2/")

    log.info("flight_time_df1 schema:")
    flight_time_df1.printSchema()

    log.info("flight_time_df2 schema:")
    flight_time_df2.printSchema()

    join_df = flight_time_df1.join(flight_time_df2, "id", "inner")

    log.info("join_df schema:")
    join_df.printSchema()

    join_df.show()

    spark.stop()
