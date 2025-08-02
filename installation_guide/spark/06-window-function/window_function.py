import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

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

    summary_df = spark.read.parquet("/data/window-function/summary.parquet")

    log.info("summary_df schema:")
    summary_df.printSchema()

    log.info("summary_df:")
    summary_df.show()

    country_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber")

    row_num_df = summary_df.withColumn("RowNum", f.row_number().over(country_window))

    log.info("row_num_df schema:")
    row_num_df.printSchema()

    log.info("row_num_df:")
    row_num_df.show()

    spark.stop()
