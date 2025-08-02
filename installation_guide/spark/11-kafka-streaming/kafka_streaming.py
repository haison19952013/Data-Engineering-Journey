import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType

from util.config import Config
from util.logger import Log4j

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    df.printSchema()

    query = df.select(col("value").cast(StringType()).alias("value")) \
        .writeStream \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
