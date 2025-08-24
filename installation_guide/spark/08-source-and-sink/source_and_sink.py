import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import functions as f
from pyspark.sql.functions import col, when

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
    
    # Question 1: Write a program to read data from the `json` directory created in the above example and get a list of canceled flights to Atlanta, GA in 2000. The data is sorted by flight date in descending order.

    flight_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .json("/data/sink/json/")
    flight_df = flight_df.withColumn("FL_DATE", f.to_timestamp(col("FL_DATE"), "yyyy-MM-dd"))
    flight_df = flight_df.withColumn("FL_YEAR", f.year(col("FL_DATE")))

    flight_df.printSchema()
    
    flight_df.show()

    q1_df = flight_df\
    .filter(
        (flight_df.DEST == 'ATL') &
        (flight_df.CANCELLED == 1) &
        (flight_df.FL_YEAR == 2000)
    )\
    .orderBy(flight_df.FL_DATE.desc())\
    .select("DEST", "DEST_CITY_NAME", "FL_DATE", "ORIGIN", "ORIGIN_CITY_NAME", "CANCELLED")

    q1_df.show()
    
    # Question 2: Write a program to read data from the `avro` directory created in the above example and get a list of carriers `OP_CARRIER`, `ORIGIN`, and total number of canceled flights. The data is sorted by `OP_CARRIER` and `ORIGIN`.
    flight_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("avro").load("/data/sink/avro/")

    q2_df = flight_df.groupBy("OP_CARRIER", "ORIGIN") \
        .agg(f.count(when(col("CANCELLED") == 1, 1)).alias("CANCELLED_COUNT")) \
        .orderBy("OP_CARRIER", "ORIGIN")
        
    q2_df.show()

    # flight_time_df = spark.read.parquet("/data/source-and-sink/flight-time.parquet")

    # flight_time_df.printSchema()

    # flight_time_df.show()

    # log.info(f"Num Partitions before: {flight_time_df.rdd.getNumPartitions()}")
    # flight_time_df.groupBy(spark_partition_id()).count().show()

    # partitioned_df = flight_time_df.repartition(5)
    # log.info(f"Num Partitions after: {partitioned_df.rdd.getNumPartitions()}")
    # partitioned_df.groupBy(spark_partition_id()).count().show()

    # partitioned_df.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "/data/sink/avro/") \
    #     .save()

    # flight_time_df.write \
    #     .format("json") \
    #     .mode("overwrite") \
    #     .partitionBy("OP_CARRIER", "ORIGIN") \
    #     .option("path", "/data/sink/json/") \
    #     .option("maxRecordsPerFile", 10000) \
    #     .save()

    spark.stop()
