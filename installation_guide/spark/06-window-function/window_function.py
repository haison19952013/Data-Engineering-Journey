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
    
    # Question 1: Write a program to get the list of countries, weeks, number of invoices, total number of products, total invoice value, and rank based on the highest total amount for each country. The data should be sorted by country name and rank in ascending order.

    country_window = Window.partitionBy("Country").orderBy(f.desc("InvoiceValue"))

    q1_df = summary_df.withColumn("Rank", f.rank().over(country_window))

    log.info("q1_df schema:")
    q1_df.printSchema()

    log.info("q1_df:")
    q1_df.show()
    
    # Question 2: Write a program to get the list of countries, weeks, number of invoices, number of products, invoice value, and cumulative invoice value up to the current week's record, percentage increase in invoice value compared to the previous week. The data should be sorted by country name and week.

    cumulative_invoice_window = Window.partitionBy("Country").orderBy("WeekNumber").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    q2_df = summary_df.withColumn("CumulativeInvoiceValue", f.sum("InvoiceValue").over(cumulative_invoice_window))

    pct_change_window = Window.partitionBy("Country").orderBy("WeekNumber")

    q2_df = q2_df.withColumn("PctChange", (f.col("InvoiceValue") - f.lag("InvoiceValue").over(pct_change_window)) / f.lag("InvoiceValue").over(pct_change_window) * 100)
    
    # Fill null values in PctChange with 0
    q2_df = q2_df.fillna({"PctChange": 0})

    log.info("q2_df schema:")
    q2_df.printSchema()

    log.info("q2_df:")
    q2_df.show()

    spark.stop()
