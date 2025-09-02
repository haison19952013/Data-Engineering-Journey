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

    # query = df.select(col("value").cast(StringType()).alias("value")) \
    #     .writeStream \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .trigger(processingTime="30 seconds") \
    #     .start()
        
    # Write a program to transform the column `value` from a JSON string to a structured row and print the converted result. Hint: Use `StructType` and the `from_json` function.
    
    # Define the schema for the JSON data
    json_schema = StructType([
        StructField("id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("local_time", StringType(), True),
        StructField("option", ArrayType(StringType()), True),
        StructField("product_id", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("time_stamp", LongType(), True),
        StructField("user_agent", StringType(), True)
    ])
    
    # Transform the JSON string to structured data
    structured_df = df.select(
        col("key").cast(StringType()).alias("key"),
        col("value").cast(StringType()).alias("json_string"),
        f.from_json(col("value").cast(StringType()), json_schema).alias("data")
    ).select(
        col("key"),
        col("json_string"),
        col("data.*")  # Expand all fields from the struct
    )
    
    # Write the structured data to console
    structured_query = structured_df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    # query.awaitTermination()
    structured_query.awaitTermination()
