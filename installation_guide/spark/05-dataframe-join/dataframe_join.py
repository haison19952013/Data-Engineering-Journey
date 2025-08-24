import os

from pyspark.sql import SparkSession

import util.config as conf
from util.logger import Log4j
from pyspark.sql import functions as f
from pyspark.sql.functions import col

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

    flight_time_df1 = flight_time_df1.withColumn("FL_DATE", f.to_timestamp(col("FL_DATE"), "M/d/yyyy"))
    flight_time_df1 = flight_time_df1.withColumn("FL_YEAR", f.year(col("FL_DATE")))
    
    log.info("flight_time_df1 schema:")
    flight_time_df1.printSchema()

    log.info("flight_time_df2 schema:")
    flight_time_df2.printSchema()

    join_df = flight_time_df1.join(flight_time_df2, on = "id", how = "inner")

    log.info("join_df schema:")
    join_df.printSchema()

    # Question 1: Write a program to get a list of canceled flights to Atlanta, GA in 2000. The data is sorted by flight date in descending order.
    canceled_atlanta_flights = join_df.filter((col("CANCELLED") == 1) & (col("DEST") == "ATL") & (col("FL_YEAR") == 2000)) \
        .orderBy(col("FL_DATE").desc())
    canceled_atlanta_flights = canceled_atlanta_flights.select("id", "DEST", "DEST_CITY_NAME", "FL_DATE", "ORIGIN", "ORIGIN_CITY_NAME", "CANCELLED")
    canceled_atlanta_flights.show()
    
    # Question 2: Write a program to get a list of destinations, year, and total number of canceled flights for that year. The data is sorted by destination code and year.
    canceled_flights_by_dest_year = join_df.filter(col("CANCELLED") == 1) \
        .groupBy("DEST", "FL_YEAR") \
        .count().withColumnRenamed("count", "NUM_CANCELLED_FLIGHT") \
        .orderBy("DEST", "FL_YEAR")
    canceled_flights_by_dest_year.show()

    spark.stop()
