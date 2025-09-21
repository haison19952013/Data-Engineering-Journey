import json
from datetime import datetime
import hashlib
import glob
import os
import pandas as pd
import logging
import time
import shutil
from urllib.parse import urlparse

from utils import (
    Logger,
    extract_browser,
    extract_os,
    convert_numpy_types,
    batch_query_existing_keys,
)
from db_connector import PostgresConnector, SparkConnector
from sql_cmd import get_create_table_sql
from config import load_config
from pyspark.sql.functions import (
    col,
    from_json,
    sha2,
    concat,
    from_unixtime,
    date_format,
)
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    LongType,
    ArrayType,
    MapType,
)
from sqlalchemy import text
from pyspark.sql.functions import to_json, col

# Suppress SQLAlchemy verbose logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.dialects").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.orm").setLevel(logging.WARNING)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/{timestamp}_streaming.log"
logger = Logger(logger_level="INFO", log_file=log_filename)


class WarmupPipeline:
    """
    A class to handle PostgreSQL database warmup operations including table creation
    and dimension table population.

    Usage:
        warmup = WarmupPipeline(test=True)
        warmup.run()
    """

    def __init__(self, test, insert_batch_size=100_000):
        self.postgres_conn = None
        self.test = test
        self.insert_batch_size = insert_batch_size

    @logger.log_errors(logger)
    def run(self):
        """
        Create fact and dimension tables in PostgreSQL if they do not exist,
        and populate dim_product, dim_location, and dim_date with data.
        """
        # Initialize PostgreSQL connector
        self.postgres_conn = PostgresConnector(
            config_name="docker_postgres",
            test=self.test,
            insert_batch_size=self.insert_batch_size,
        )

        # Get SQL commands for creating tables
        sql_commands = get_create_table_sql()

        # Create each table
        for table_name, create_sql in sql_commands.items():
            logger.info(f"Creating table: {table_name}")
            self.postgres_conn.create_table(create_sql)

        # Insert data into dim_product
        self.insert_dim_product()

        # Insert data into dim_location
        self.insert_dim_location()

        # Insert data into dim_date
        self.insert_dim_date()

    def insert_dim_product(self):
        """Insert product data from CSV files into dim_product table."""
        data_config = load_config(filename="config.ini", section="data_paths")
        base_path = data_config["base_path"]
        pattern = os.path.join(base_path, data_config["prod_csv_pattern"])
        csv_files = glob.glob(pattern)

        total_inserted = 0
        for csv_file in csv_files:
            logger.info(f"Loading data from {csv_file}")
            df = pd.read_csv(csv_file)

            # Select and rename columns
            df = df[["product_id", "name", "url", "language_code"]].rename(
                columns={"product_id": "product_key", "url": "current_url"}
            )
            # Convert product_key to string to match VARCHAR database type
            df["product_key"] = df["product_key"].astype(str)
            # Remove duplicates within this file
            df = df.drop_duplicates(subset=["product_key"])

            if not df.empty:
                # Check for existing product_keys in database
                engine = self.postgres_conn.get_engine()
                unique_products = df["product_key"].unique()

                if len(unique_products) > 0:
                    with engine.connect() as conn:
                        # Convert numpy types to native Python types and ensure strings
                        product_list = [
                            str(x) for x in convert_numpy_types(list(unique_products))
                        ]
                        # Use batch querying to avoid memory issues
                        existing_keys = batch_query_existing_keys(
                            conn, "dim_product", "product_key", product_list
                        )
                else:
                    existing_keys = set()

                # Filter out existing products
                new_products_df = df[~df["product_key"].isin(existing_keys)]

                if not new_products_df.empty:
                    self.postgres_conn.insert_record("dim_product", new_products_df)
                    total_inserted += len(new_products_df)
                    logger.info(
                        f"Inserted {len(new_products_df)} new product records from {csv_file}"
                    )
                else:
                    logger.info(
                        f"No new products to insert from {csv_file} (all already exist)"
                    )

        logger.info(f"Total inserted {total_inserted} product records")

    def insert_dim_location(self):
        """Insert location data from CSV files into dim_location table."""
        data_config = load_config(filename="config.ini", section="data_paths")
        base_path = data_config["base_path"]
        pattern = os.path.join(base_path, data_config["location_csv_pattern"])
        csv_files = glob.glob(pattern)

        total_inserted = 0
        for csv_file in csv_files:
            logger.info(f"Loading data from {csv_file}")
            df = pd.read_csv(csv_file)
            # Compute ip_key
            df["ip_key"] = df["ip"].apply(
                lambda x: hashlib.sha256(x.encode()).hexdigest()
            )
            # Select and rename columns
            df = df[
                ["ip_key", "ip", "country_short", "country_long", "region", "city"]
            ].rename(
                columns={
                    "ip": "ip_address",
                    "country_short": "country_code",
                    "country_long": "country_name",
                    "region": "region_name",
                    "city": "city_name",
                }
            )
            # Remove duplicates within this file
            df = df.drop_duplicates(subset=["ip_key"])

            if not df.empty:
                # Check for existing ip_keys in database
                engine = self.postgres_conn.get_engine()
                unique_ip_keys = df["ip_key"].unique()

                if len(unique_ip_keys) > 0:
                    with engine.connect() as conn:
                        # Convert numpy types to native Python types
                        ip_list = convert_numpy_types(list(unique_ip_keys))
                        # Use batch querying to avoid memory issues
                        existing_keys = batch_query_existing_keys(
                            conn, "dim_location", "ip_key", ip_list
                        )
                else:
                    existing_keys = set()

                # Filter out existing locations
                new_locations_df = df[~df["ip_key"].isin(existing_keys)]

                if not new_locations_df.empty:
                    self.postgres_conn.insert_record("dim_location", new_locations_df)
                    total_inserted += len(new_locations_df)
                    logger.info(
                        f"Inserted {len(new_locations_df)} new location records from {csv_file}"
                    )
                else:
                    logger.info(
                        f"No new locations to insert from {csv_file} (all already exist)"
                    )

        logger.info(f"Total inserted {total_inserted} location records")

    def insert_dim_date(self):
        """Insert date dimension data from 2010 to 2030 into dim_date table."""
        logger.info("Generating date dimension data from 2010 to 2030")

        # Generate date range
        dates = pd.date_range("2010-01-01", "2030-12-31", freq="D")

        # Create DataFrame with date fields
        df = pd.DataFrame(
            {
                "full_date": dates.strftime("%Y-%m-%d"),
                "day_of_week": dates.weekday + 1,  # Monday=1, Sunday=7
                "day_of_week_short": dates.strftime("%a"),
                "is_weekday_or_weekend": [
                    "Weekday" if x < 5 else "Weekend" for x in dates.weekday
                ],
                "day_of_month": dates.day,
                "year_month": dates.strftime("%Y-%m"),
                "month": dates.month,
                "day_of_year": dates.dayofyear,
                "week_of_year": dates.isocalendar().week,
                "quarter_number": dates.quarter,
                "year": dates.year,
            }
        )

        self.postgres_conn.insert_record("dim_date", df)

        logger.info(f"Total inserted {len(df)} date records")


class StreamingPipeline:
    """
    A class to manage the end-to-end streaming data pipeline from Kafka using Spark to PostgreSQL.

    """

    def __init__(self, test=False):
        self.spark_conn = None
        self.postgres_conn = None
        self.spark = None
        self.test = test

    @logger.log_errors(logger)
    def start_streaming(self, processing_mode="foreachPartition"):
        """
        Start the streaming pipeline from Kafka to PostgreSQL.

        Args:
            test (bool): If True, use local Spark configuration for testing.
            processing_mode (str): "foreachPartition" or "foreachBatch" - choose processing approach
        """
        if self.test:
            # Clean up old checkpoint directories to prevent issues
            checkpoint_dirs = [
                "/tmp/spark_checkpoints/streaming_pipeline",
                "/tmp/spark_checkpoints/streaming_pipeline_batch",
            ]
            for checkpoint_dir in checkpoint_dirs:
                if os.path.exists(checkpoint_dir):
                    try:
                        shutil.rmtree(checkpoint_dir)
                        logger.info(
                            f"Cleaned up old checkpoint directory: {checkpoint_dir}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Could not clean checkpoint directory {checkpoint_dir}: {e}"
                        )

        # Initialize Spark connector
        if self.test:
            self.spark_conn = SparkConnector(config_name="local_spark")
        else:
            self.spark_conn = SparkConnector(config_name="docker_spark")
        self.spark = self.spark_conn.create_spark_session()

        # Initialize PostgreSQL connector
        self.postgres_conn = PostgresConnector(
            config_name="docker_postgres", test=self.test, insert_batch_size=10_000
        )

        # Load Kafka configuration
        kafka_conf = load_config(filename="config.ini", section="remote_kafka")

        # Define the schema for the JSON data from Kafka
        schema = StructType(
            [
                StructField("_id", StringType()),
                StructField("time_stamp", LongType()),
                StructField("ip", StringType()),
                StructField("user_agent", StringType()),
                StructField("resolution", StringType()),
                StructField("user_id_db", StringType()),
                StructField("device_id", StringType()),
                StructField("api_version", StringType()),
                StructField("store_id", StringType()),
                StructField("local_time", StringType()),
                StructField("show_recommendation", StringType()),
                StructField("current_url", StringType()),
                StructField("referrer_url", StringType()),
                StructField("email_address", StringType()),
                StructField("recommendation", StringType()),
                StructField("utm_source", StringType()),
                StructField("utm_medium", StringType()),
                StructField("collection", StringType()),
                StructField("product_id", StringType()),
                StructField("option", ArrayType(MapType(StringType(), StringType()))),
                StructField("id", StringType()),
            ]
        )

        # Read from Kafka
        df = self.spark.readStream.format("kafka").options(**kafka_conf).load()

        # Parse JSON and convert to pandas
        parsed_df = df.select(
            from_json(col("value").cast(StringType()), schema).alias("data")
        ).select("data.*")
        parsed_df = parsed_df.withColumn("option", to_json(col("option")))

        if processing_mode == "foreachPartition":
            # Function to write partition to PostgreSQL
            def write_to_postgres(partition_iter):

                start_time = time.time()
                buffer = []
                batch_size = 5000

                try:
                    # Collect partition data into a list
                    for row in partition_iter:
                        buffer.append(row.asDict())
                        if len(buffer) >= batch_size:
                            pandas_df = pd.DataFrame(buffer)
                            self.load(self.transform(pandas_df))
                            buffer.clear()
                    if buffer:
                        pandas_df = pd.DataFrame(buffer)
                        self.load(self.transform(pandas_df))

                    end_time = time.time()
                    logger.info(
                        f"Partition processing completed in {end_time - start_time:.2f} seconds"
                    )
                except Exception as e:
                    logger.error(f"Error processing partition: {str(e)}")
                    # Continue processing other partitions

            # Function to process batch and repartition for foreachPartition
            def process_batch_with_repartition(batch_df, epoch_id):
                """Process batch by repartitioning and using foreachPartition."""
                try:
                    # Use isEmpty() instead of count() for better performance
                    if not batch_df.isEmpty():
                        logger.info(
                            f"Processing batch {epoch_id} using repartition + foreachPartition"
                        )

                        # Cache the dataframe to avoid recomputation
                        batch_df.cache()

                        # Repartition the batch DataFrame and apply foreachPartition
                        batch_df.repartition(4).foreachPartition(write_to_postgres)

                        # Unpersist to free memory
                        batch_df.unpersist()
                    else:
                        logger.info(f"Batch {epoch_id} is empty, skipping processing")
                except Exception as e:
                    logger.error(f"Error processing batch {epoch_id}: {str(e)}")
                    # Continue processing other batches

            # Start the streaming query with foreachBatch that uses repartition + foreachPartition
            query = (
                parsed_df.writeStream.foreachBatch(process_batch_with_repartition)
                .option(
                    "checkpointLocation", "/tmp/spark_checkpoints/streaming_pipeline"
                )
                .trigger(processingTime="30 seconds")
                .start()
            )

        elif processing_mode == "foreachBatch":
            # Function to process batch data
            def process_batch(batch_df, epoch_id):
                """Process each micro-batch of data."""
                start_time = time.time()

                try:
                    # Use isEmpty() instead of count() for better performance
                    if not batch_df.isEmpty():
                        logger.info(f"Processing batch {epoch_id} using foreachBatch")

                        # Cache the dataframe to avoid recomputation
                        batch_df.cache()

                        # Convert to pandas DataFrame
                        pandas_df = batch_df.toPandas()

                        # Unpersist the Spark DataFrame to free memory
                        batch_df.unpersist()

                        logger.info(
                            f"Converted batch {epoch_id} to pandas with {len(pandas_df)} records"
                        )

                        # Transform: Add computed columns
                        pandas_df = self.transform(pandas_df)

                        # Load: Insert missing dimensions and filter existing facts
                        self.load(pandas_df)

                        end_time = time.time()
                        logger.info(
                            f"Completed processing batch {epoch_id} with {len(pandas_df)} records in {end_time - start_time:.2f} seconds"
                        )
                    else:
                        logger.info(f"Batch {epoch_id} is empty, skipping processing")
                except Exception as e:
                    logger.error(f"Error processing batch {epoch_id}: {str(e)}")
                    # Continue processing other batches

            # Start the streaming query with foreachBatch
            query = (
                parsed_df.writeStream.foreachBatch(process_batch)
                .option(
                    "checkpointLocation",
                    "/tmp/spark_checkpoints/streaming_pipeline_batch",
                )
                .trigger(processingTime="30 seconds")
                .start()
            )

        else:
            raise ValueError(
                "processing_mode must be either 'foreachPartition' or 'foreachBatch'"
            )

        logger.info(
            f"Streaming pipeline started with {processing_mode} mode. Press Ctrl+C to stop."
        )

        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info(
                "Received interrupt signal. Stopping streaming pipeline gracefully..."
            )
            query.stop()
            self.spark.stop()
            logger.info("Streaming pipeline stopped successfully.")

    def transform(self, df):
        """Transform raw data by adding computed columns."""

        # Convert timestamp to datetime (assuming seconds since epoch)
        df["timestamp_dt"] = pd.to_datetime(df["time_stamp"], unit="s")

        # Create date and time columns
        df["full_date"] = df["timestamp_dt"].dt.strftime("%Y-%m-%d")
        df["full_time"] = df["timestamp_dt"].dt.strftime("%H:%M:%S")

        # Create hash keys
        df["sales_key"] = df.apply(
            lambda row: hashlib.sha256(
                f"{row['id']}{row['product_id']}".encode()
            ).hexdigest(),
            axis=1,
        )
        df["ip_key"] = df["ip"].apply(
            lambda x: hashlib.sha256(x.encode()).hexdigest() if pd.notna(x) else None
        )
        df["user_agent_key"] = df["user_agent"].apply(
            lambda x: hashlib.sha256(x.encode()).hexdigest() if pd.notna(x) else None
        )
        df["product_key"] = df["product_id"].apply(
            lambda x: str(x) if pd.notna(x) else None
        )

        # Extract browser and OS from user_agent
        df["browser"] = df["user_agent"].apply(extract_browser)
        df["os"] = df["user_agent"].apply(extract_os)

        # Convert option array to string
        df["option"] = df["option"].apply(lambda x: str(x) if x is not None else None)
        # For refer_url, only need to truncate to the domain. For example, from "http://example.com/page" to "example.com"

        df["referrer_url"] = df["referrer_url"].apply(
            lambda x: (
                urlparse("http://" + x).netloc
                if pd.notna(x) and x.strip() != "" and "//" not in x
                else urlparse(x).netloc if pd.notna(x) and x.strip() != ""
                else None
            )
        )

        return df

    def load(self, fact_df):
        """Insert missing dimension records and filter out existing fact records."""

        # If the dataframe is too large, process in chunks to avoid memory issues
        if len(fact_df) > 5000:
            logger.info(
                f"Large dataset detected ({len(fact_df)} records). Processing in chunks of 5000."
            )
            chunk_size = 5000
            for i in range(0, len(fact_df), chunk_size):
                chunk = fact_df.iloc[i : i + chunk_size]
                logger.info(
                    f"Processing chunk {i//chunk_size + 1} with {len(chunk)} records"
                )
                self._load_chunk(chunk)
            return
        else:
            self._load_chunk(fact_df)

    def _load_chunk(self, fact_df):
        """Process a chunk of fact data."""
        engine = self.postgres_conn.get_engine()

        # Get unique product_keys
        unique_products = fact_df["product_key"].dropna().unique()
        if len(unique_products) > 0:
            with engine.connect() as conn:
                # Check existing product_keys using batch querying
                # Convert to strings to match VARCHAR database type
                product_list = [
                    str(x) for x in convert_numpy_types(list(unique_products))
                ]
                existing_keys = batch_query_existing_keys(
                    conn, "dim_product", "product_key", product_list
                )

                # Insert missing products
                missing_products = [
                    key for key in unique_products if key not in existing_keys
                ]
                if missing_products:
                    product_records = [
                        {
                            "product_key": key,
                            "name": None,
                            "current_url": None,
                            "language_code": None,
                        }
                        for key in missing_products
                    ]
                    self.postgres_conn.insert_record("dim_product", product_records)

        # Get unique ip_keys and corresponding ip_addresses
        unique_ips = fact_df[["ip_key", "ip"]].dropna().drop_duplicates()
        unique_ip_keys = unique_ips["ip_key"].unique()
        if len(unique_ip_keys) > 0:
            with engine.connect() as conn:
                # Check existing ip_keys using batch querying
                ip_list = convert_numpy_types(list(unique_ip_keys))
                existing_keys = batch_query_existing_keys(
                    conn, "dim_location", "ip_key", ip_list
                )

                # Insert missing locations
                missing_ip_keys = [
                    key for key in unique_ip_keys if key not in existing_keys
                ]
                if missing_ip_keys:
                    # Create mapping from ip_key to ip_address
                    ip_mapping = dict(zip(unique_ips["ip_key"], unique_ips["ip"]))
                    location_records = [
                        {
                            "ip_key": key,
                            "ip_address": ip_mapping[key],
                            "country_code": None,
                            "country_name": None,
                            "region_name": None,
                            "city_name": None,
                        }
                        for key in missing_ip_keys
                    ]
                    self.postgres_conn.insert_record("dim_location", location_records)

        # Get unique user_agent_keys and corresponding user_agents
        unique_uas = (
            fact_df[["user_agent_key", "user_agent", "browser", "os"]]
            .dropna()
            .drop_duplicates()
        )
        if not unique_uas.empty:
            with engine.connect() as conn:
                # Check existing user_agent_keys
                ua_keys = list(unique_uas["user_agent_key"])
                if ua_keys:
                    # Check existing user_agent_keys using batch querying
                    ua_list = convert_numpy_types(ua_keys)
                    existing_keys = batch_query_existing_keys(
                        conn, "dim_user_agent", "user_agent_key", ua_list
                    )

                    # Insert missing user agents
                    missing_uas = unique_uas[
                        ~unique_uas["user_agent_key"].isin(existing_keys)
                    ]
                    if not missing_uas.empty:
                        ua_records = missing_uas.to_dict("records")
                        self.postgres_conn.insert_record("dim_user_agent", ua_records)

        # Filter out existing fact records based on sales_key
        unique_sales_keys = fact_df["sales_key"].dropna().unique()
        if len(unique_sales_keys) > 0:
            with engine.connect() as conn:
                # Check existing sales_keys using batch querying
                sales_list = convert_numpy_types(list(unique_sales_keys))
                existing_keys = batch_query_existing_keys(
                    conn, "fact_sales", "sales_key", sales_list
                )

                # Filter out existing sales_keys
                fact_df = fact_df[~fact_df["sales_key"].isin(existing_keys)]

        # Insert fact records if any remain after filtering
        if not fact_df.empty:
            # Select columns for fact_sales table
            fact_columns = [
                "sales_key",
                "full_date",
                "full_time",
                "ip_key",
                "user_agent_key",
                "product_key",
                "referrer_url",
                "collection",
                "option",
                "email_address",
                "resolution",
                "user_id_db",
                "device_id",
                "api_version",
                "store_id",
                "local_time",
                "show_recommendation",
                "recommendation",
                "utm_source",
                "utm_medium",
            ]
            fact_data = fact_df[fact_columns]
            self.postgres_conn.insert_record("fact_sales", fact_data)

    @logger.log_errors(logger)
    def run(self, processing_mode="foreachPartition"):
        """
        Run the streaming pipeline from Kafka to PostgreSQL.

        Args:
            test (bool): If True, use local Spark configuration for testing.
            processing_mode (str): "foreachPartition" or "foreachBatch" - choose processing approach
        """
        logger.info(
            f"Starting Streaming Pipeline: Kafka -> Spark -> PostgreSQL (Mode: {processing_mode})"
        )

        # Start the streaming process
        self.start_streaming(processing_mode=processing_mode)


if __name__ == "__main__":
    # Warmup Pipeline
    if False:  # Change to True to run warmup
        warmup = WarmupPipeline(test=True)
        warmup.run()
        exit()

    # # Stream Pipeline
    if True:  # Change to True to run stream
        stream = StreamingPipeline(test=True)
        processing_mode = "foreachBatch"  # Choose your processing mode: "foreachPartition" or "foreachBatch"
        stream.run(processing_mode=processing_mode)
