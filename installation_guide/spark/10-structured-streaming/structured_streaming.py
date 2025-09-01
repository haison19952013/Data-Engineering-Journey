import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from util.config import Config
from util.logger import Log4j

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    nc_conf = conf.nc_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"nc_conf: {nc_conf}")

    socket_df = spark \
        .readStream \
        .format("socket") \
        .option("host", nc_conf.host) \
        .option("port", nc_conf.port) \
        .load()

    log.info(f"isStreaming: {socket_df.isStreaming}")

    socket_df.printSchema()

    count_df = socket_df \
        .withColumn("word", f.explode(f.split("value", " "))) \
        .groupBy("word") \
        .agg(f.count("*").alias("count"))
    
    # Question 1: Write a program to count words and print the list of words that appear an even number of times.
    even_words_df = count_df.filter(f.col("count") % 2 == 0)

    streaming_query = even_words_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime="20 seconds") \
        .start()
    
    # Question 2: Write a program to count words and print the list of words that have a length greater than 1 and appear an odd number of times.
    odd_length_words_df = count_df.filter((f.col("count") % 2 == 1) & (f.length(f.col("word")) > 1))
    odd_length_streaming_query = odd_length_words_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime="20 seconds") \
        .start()

    streaming_query.awaitTermination()
