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

    streaming_query = count_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime="20 seconds") \
        .start()

    streaming_query.awaitTermination()
