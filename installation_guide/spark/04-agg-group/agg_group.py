import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()

    spark = SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    invoice_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/agg-group/invoices.csv")

    log.info("invoice_df schema:")
    invoice_df.printSchema()

    num_records = f.count("*").alias("num_records")
    total_quantity = f.sum("Quantity").alias("total_quantity")
    avg_price = f.avg("UnitPrice").alias("avg_price")
    num_invoices = f.count_distinct('InvoiceNo').alias("num_invoices")
    invoice_value = f.round(f.sum(col("Quantity") * col("UnitPrice")), 2).alias("invoice_value")

    agg_df = invoice_df.select(
        num_records,
        total_quantity,
        avg_price,
        num_invoices,
        invoice_value)

    log.info("agg_df:")
    agg_df.show()

    group_df = invoice_df \
        .groupBy("Country") \
        .agg(num_invoices, total_quantity, invoice_value) \
        .orderBy(col("invoice_value").desc(), col("Country"))

    log.info("group_df:")
    group_df.show()

    spark.stop()
