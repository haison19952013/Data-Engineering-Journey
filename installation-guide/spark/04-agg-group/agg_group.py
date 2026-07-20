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
    
    # Question 1: Write a program to get the list of countries, years, number of invoices, quantity of products, total amount for each country and year
    # First, we need to add a new column "Year" to the DataFrame, InvoiceDate is in string format, we need to convert it to date format first
    # then extract the year from the date format
    # InvoiceDate format: 01-12-2010 8.26

    invoice_df = invoice_df.withColumn("InvoiceDate", f.to_timestamp(col("InvoiceDate"), "dd-MM-yyyy H.mm"))
    invoice_df = invoice_df.withColumn("Year", f.year(col("InvoiceDate")))
    invoice_df.show(5)

    num_records = f.count("*").alias("num_records")
    total_quantity = f.sum("Quantity").alias("total_quantity")
    avg_price = f.avg("UnitPrice").alias("avg_price")
    num_invoices = f.count_distinct('InvoiceNo').alias("num_invoices")
    invoice_value = f.round(f.sum(col("Quantity") * col("UnitPrice")), 2).alias("invoice_value")

    group_df = invoice_df \
        .groupBy("Country", "Year") \
        .agg(num_invoices, total_quantity, invoice_value) \
        .orderBy(col("Country"), col("Year"))

    log.info("group_df:")
    group_df.show()
    
    # Question 2: Write a program to get the top 10 customers who spent the most in 2010. The data is sorted by amount in descending order, if the amount is equal, then sort by customer ID in ascending order

    top_customers_df = invoice_df \
        .filter((col("Year") == 2010) & (col('CustomerID').isNotNull())) \
        .groupBy("CustomerID") \
        .agg(invoice_value) \
        .orderBy(col("invoice_value").desc(), col("CustomerID").asc()) \
        .limit(10)

    log.info("top_customers_df:")
    top_customers_df.show()

    spark.stop()
