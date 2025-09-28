from datetime import datetime
import hashlib
import glob
import os
import pandas as pd
import time
import shutil
import logging

from .utils import (
    Logger,
    convert_numpy_types,
    get_table_creation_sql,
    get_kafka_json_schema,
    get_udfs
)
from .db_connector import PostgresConnector, SparkConnector
from .config import load_config
from pyspark.sql.functions import (
    col,
    from_json,
    sha2,
    concat,
    from_unixtime,
    date_format,
    when,
    lit,
    to_json
)
from pyspark.sql.types import StringType


# Suppress SQLAlchemy verbose logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.dialects").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.orm").setLevel(logging.WARNING)
logging.getLogger("psycopg2").setLevel(logging.WARNING)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"../logs/{timestamp}_streaming.log"
logger = Logger(logger_level="INFO", log_file=None)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.ini")

class WarmupPipeline:
    """
    A class to handle PostgreSQL database warmup operations including table creation
    and dimension table population.

    Method Organization:
    1. Constructor and initialization
    2. Main orchestration method
    3. Helper methods for dimension table population

    Usage:
        warmup = WarmupPipeline(test=True)
        warmup.run()
    """

    def __init__(self, test_mode, insert_batch_size=100_000):
        """Initialize the WarmupPipeline."""
        self.postgres_conn = None
        self.test_mode = test_mode
        self.insert_batch_size = insert_batch_size

    # ===== MAIN ORCHESTRATION METHOD =====

    @logger.log_errors(logger)
    def run(self):
        """
        Create fact and dimension tables in PostgreSQL if they do not exist,
        and populate dim_product, dim_location, and dim_date with data.
        """
        # Initialize PostgreSQL connector
        self.postgres_conn = PostgresConnector(
            config_name="docker_postgres",
            test_mode=self.test_mode,
            insert_batch_size=self.insert_batch_size,
        )

        # Get SQL commands for creating tables
        sql_commands = get_table_creation_sql()

        # Create each table
        for table_name, create_sql in sql_commands.items():
            logger.info(f"Creating table: {table_name}")
            self.postgres_conn.create_table(create_table_sql=create_sql)

        # Insert data into dimension tables
        self.insert_dim_product()
        self.insert_dim_location()
        self.insert_dim_date()

    # ===== HELPER METHODS FOR DIMENSION TABLE POPULATION =====

    def insert_dim_product(self):
        """Insert product data from CSV files into dim_product table."""
        data_config = load_config(filename=CONFIG_PATH, section="data_paths")
        base_path = data_config["base_path"]
        pattern = os.path.join(base_path, data_config["prod_csv_pattern"])
        csv_files = glob.glob(pattern)

        total_processed = 0
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
                # Insert records (database handles duplicates with ON CONFLICT DO NOTHING)
                self.postgres_conn.insert_record(
                    table_name="dim_product", 
                    records=df, 
                    if_exists='append', 
                    use_on_conflict=True
                )
                total_processed += len(df)
                logger.info(
                    f"Processed {len(df)} product records from {csv_file} (database handles duplicates)"
                )

        logger.info(f"Total processed {total_processed} product records")

    def insert_dim_location(self):
        """Insert location data from CSV files into dim_location table."""
        data_config = load_config(filename=CONFIG_PATH, section="data_paths")
        base_path = data_config["base_path"]
        pattern = os.path.join(base_path, data_config["location_csv_pattern"])
        csv_files = glob.glob(pattern)

        total_processed = 0
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
                # Insert records (database handles duplicates with ON CONFLICT DO NOTHING)
                self.postgres_conn.insert_record(
                    table_name="dim_location", 
                    records=df, 
                    if_exists='append', 
                    use_on_conflict=True
                )
                total_processed += len(df)
                logger.info(
                    f"Processed {len(df)} location records from {csv_file} (database handles duplicates)"
                )

        logger.info(f"Total processed {total_processed} location records")

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

        self.postgres_conn.insert_record(
            table_name="dim_date", 
            records=df, 
            if_exists='append', 
            use_on_conflict=True
        )

        logger.info(f"Total inserted {len(df)} date records")


class StreamingPipeline:
    """
    A class to manage the end-to-end streaming data pipeline from Kafka using Spark to PostgreSQL.

    Method Organization:
    1. Constructor and initialization
    2. ETL pipeline methods (extract, transform, load)
    3. Helper methods for table loading (static)
    4. Processing mode methods (static)
    5. Main orchestration method
    """

    def __init__(self, test_mode=False):
        """Initialize the StreamingPipeline."""
        self.spark_conn = None
        self.postgres_conn = None
        self.spark = None
        self.test_mode = test_mode
        
    @logger.log_errors(logger)
    def initialize_connections(self):
        """Initialize Spark and Kafka connections."""
        if self.test_mode:
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
        if self.test_mode:
            self.spark_conn = SparkConnector(config_name="local_spark")
        else:
            self.spark_conn = SparkConnector(config_name="docker_spark")
        self.spark = self.spark_conn.create_spark_session()
        
        # Access log4j from JVM and set level just for KafkaDataConsumer
        log4j = self.spark._jvm.org.apache.log4j
        log4j.LogManager.getLogger("org.apache.spark.sql.kafka010.KafkaDataConsumer").setLevel(log4j.Level.ERROR)

        # Load Kafka configuration
        self.kafka_conf = load_config(filename=CONFIG_PATH, section="remote_kafka")

    # ===== ETL PIPELINE METHODS =====
    
    @logger.log_errors(logger)
    def extract(self):
        """Extract data from Kafka."""
        # Read from Kafka
        df = self.spark.readStream.format("kafka").options(**self.kafka_conf).load()
        return df

    def transform(self, df, schema):
        """Transform raw Kafka data using Spark operations for better performance."""

        # Parse JSON and extract data
        df = df.select(
            from_json(col("value").cast(StringType()), schema).alias("data")
        ).select("data.*")

        # Transform data in Spark for better performance
        # 1. Convert option array to JSON string
        df = df.withColumn("option", to_json(col("option")))

        # 2. Generate timestamp-based columns
        df = df.withColumn(
            "timestamp_dt", from_unixtime(col("time_stamp")))
        df = df.withColumn(
            "full_date", date_format(col("timestamp_dt"), "yyyy-MM-dd"))
        df = df.withColumn(
            "full_time", date_format(col("timestamp_dt"), "HH:mm:ss"))

        # 3. Generate hash keys
        df = df.withColumn(
            "sales_key",
            sha2(concat(
                col("id"), 
                when(col("product_id").isNotNull(), col("product_id")).otherwise(lit(""))
            ), 256)
        )
        df = df.withColumn(
            "ip_key",
            when(col("ip").isNotNull(), sha2(
                col("ip"), 256)).otherwise(lit(None))
        )
        df = df.withColumn(
            "user_agent_key",
            when(col("user_agent").isNotNull(), sha2(
                col("user_agent"), 256)).otherwise(lit(None))
        )

        # 4. Convert product_id to product_key as string
        df = df.withColumn(
            "product_key",
            when(col("product_id").isNotNull(), col("product_id").cast("string"))
            .otherwise(lit(None))  # Keep as null if product_id is null
        )

        # 5. Clean referrer_url to extract domain using UDF
        extract_referrer_domain_udf = get_udfs(udf_name='extract_referrer_domain')
        df = df.withColumn(
            "referrer_url",
            extract_referrer_domain_udf(col("referrer_url"))
        )

        # 6. Extract browser and OS from user_agent using UDFs
        extract_browser_udf = get_udfs(udf_name='extract_browser')
        extract_os_udf = get_udfs(udf_name='extract_os')
        df = df.withColumn(
            "browser",
            extract_browser_udf(col("user_agent"))
        )
        df = df.withColumn(
            "os",
            extract_os_udf(col("user_agent"))
        )

        return df

    def load(self, fact_df, load_batch=10_000):
        """Process a chunk of fact data using static methods for each table."""
        
        postgres_conn = PostgresConnector(
            config_name="docker_postgres", 
            test_mode=self.test_mode, 
            insert_batch_size=load_batch
        )
        
        # Load dimension tables
        self._load_dim_product(fact_df=fact_df, postgres_conn=postgres_conn)
        self._load_dim_location(fact_df=fact_df, postgres_conn=postgres_conn)
        self._load_dim_user_agent(fact_df=fact_df, postgres_conn=postgres_conn)
        
        # Load fact table
        self._load_fact_sales(fact_df=fact_df, postgres_conn=postgres_conn)

    # ===== HELPER METHODS FOR TABLE LOADING =====
    
    @staticmethod
    def _load_dim_product(fact_df, postgres_conn):
        """Load dimension data for products."""
        unique_products = fact_df["product_key"].dropna().drop_duplicates()
        if len(unique_products) > 0:
            # Convert to strings to match VARCHAR database type
            product_list = [
                str(x) for x in convert_numpy_types(obj=list(unique_products))
            ]
            product_records = [
                {
                    "product_key": key,
                    "name": None,
                    "current_url": None,
                    "language_code": None,
                }
                for key in product_list
            ]
            postgres_conn.insert_record(
                table_name="dim_product", 
                records=product_records, 
                if_exists='append', 
                use_on_conflict=True
            )

    @staticmethod
    def _load_dim_location(fact_df, postgres_conn):
        """Load dimension data for locations."""
        unique_ips = fact_df[["ip_key", "ip"]].dropna().drop_duplicates()
        if not unique_ips.empty:
            # Create mapping from ip_key to ip_address  
            location_records = [
                {
                    "ip_key": row["ip_key"],
                    "ip_address": row["ip"],
                    "country_code": None,
                    "country_name": None,
                    "region_name": None,
                    "city_name": None,
                }
                for _, row in unique_ips.iterrows()
            ]
            postgres_conn.insert_record(
                table_name="dim_location", 
                records=location_records, 
                if_exists='append', 
                use_on_conflict=True
            )

    @staticmethod
    def _load_dim_user_agent(fact_df, postgres_conn):
        """Load dimension data for user agents."""
        unique_uas = (
            fact_df[["user_agent_key", "user_agent", "browser", "os"]]
            .dropna()
            .drop_duplicates()
        )
        if not unique_uas.empty:
            ua_records = unique_uas.to_dict("records")
            postgres_conn.insert_record(
                table_name="dim_user_agent", 
                records=ua_records, 
                if_exists='append', 
                use_on_conflict=True
            )

    @staticmethod
    def _load_fact_sales(fact_df, postgres_conn):
        """Load fact data for sales."""
        unique_sales = fact_df.dropna(subset=["sales_key"]).drop_duplicates()
        if not unique_sales.empty:
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
            sales_records = unique_sales[fact_columns]
            postgres_conn.insert_record(
                table_name="fact_sales", 
                records=sales_records, 
                if_exists='append', 
                use_on_conflict=True
            )

    # ===== PROCESSING MODE METHODS =====
    
    @staticmethod
    def _process_with_foreach_partition(df, load_func):
        """Process streaming data using foreachPartition mode."""
        
        # Function to write partition to PostgreSQL
        def write_to_postgres(partition_iter):
            start_time = time.time()

            try:
                # Collect partition data into a list
                partition_data = list(partition_iter)

                if not partition_data:
                    return

                # Convert to pandas DataFrame
                pandas_df = pd.DataFrame(
                    [row.asDict() for row in partition_data])

                # Load the data (transform already done in Spark)
                load_func(pandas_df)

                end_time = time.time()
                logger.info(
                    f"Partition processing completed {len(pandas_df)} records in {end_time - start_time:.2f} seconds"
                )
            except Exception as e:
                logger.error(f"Error processing partition: {str(e)}")
                # Continue processing other partitions

        # Function to process batch and repartition for foreachPartition
        def process_batch_with_repartition(df, epoch_id):
            """Process batch by repartitioning and using foreachPartition."""
            try:
                # Use isEmpty() instead of count() for better performance
                if not df.isEmpty():
                    logger.info(
                        f"Processing batch {epoch_id} using repartition + foreachPartition"
                    )

                    # Cache the dataframe to avoid recomputation
                    df.cache()

                    # Repartition the batch DataFrame and apply foreachPartition
                    df.repartition(2).foreachPartition(write_to_postgres)

                    # Unpersist to free memory
                    df.unpersist()
                else:
                    logger.info(
                        f"Batch {epoch_id} is empty, skipping processing")
            except Exception as e:
                logger.error(
                    f"Error processing batch {epoch_id}: {str(e)}")
                # Continue processing other batches

        # Start the streaming query with foreachBatch that uses repartition + foreachPartition
        query = (
            df.writeStream.foreachBatch(process_batch_with_repartition)
            .option(
                "checkpointLocation", "/tmp/spark_checkpoints/streaming_pipeline"
            )
            .trigger(processingTime="30 seconds")
            .start()
        )
        
        return query

    @staticmethod
    def _process_with_foreach_batch(parsed_df, load_func):
        """Process streaming data using foreachBatch mode."""
        
        # Function to process batch data
        def process_batch(batch_df, epoch_id):
            """Process each micro-batch of data."""
            start_time = time.time()

            try:
                # Use isEmpty() instead of count() for better performance
                if not batch_df.isEmpty():
                    logger.info(
                        f"Processing batch {epoch_id} using foreachBatch")

                    # Cache the dataframe to avoid recomputation
                    batch_df.cache()

                    # Convert to pandas DataFrame
                    pandas_df = batch_df.toPandas()

                    # Unpersist the Spark DataFrame to free memory
                    batch_df.unpersist()

                    logger.info(
                        f"Converted batch {epoch_id} to pandas with {len(pandas_df)} records"
                    )

                    # Load: Insert missing dimensions and filter existing facts (transform already done in Spark)
                    load_func(pandas_df)

                    end_time = time.time()
                    logger.info(
                        f"Completed processing batch {epoch_id} with {len(pandas_df)} records in {end_time - start_time:.2f} seconds"
                    )
                else:
                    logger.info(
                        f"Batch {epoch_id} is empty, skipping processing")
            except Exception as e:
                logger.error(
                    f"Error processing batch {epoch_id}: {str(e)}")
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
        
        return query

    # ===== MAIN ORCHESTRATION METHOD =====
    
    @logger.log_errors(logger)
    def run(self, processing_mode="foreachPartition"):
        """
        Start the streaming pipeline from Kafka to PostgreSQL.

        Args:
            processing_mode (str): "foreachPartition" or "foreachBatch" - choose processing approach
        """
        logger.info(
            f"Starting Streaming Pipeline: Kafka -> Spark -> PostgreSQL (Mode: {processing_mode})"
        )
        # Initialize connections
        self.initialize_connections()    
        # Read from Kafka
        df = self.extract() 
        # Apply Spark transformations
        parsed_df = self.transform(df=df, schema=get_kafka_json_schema())

        # Choose processing mode
        if processing_mode == "foreachPartition":
            query = self._process_with_foreach_partition(df=parsed_df, load_func=self.load)
        elif processing_mode == "foreachBatch":
            query = self._process_with_foreach_batch(parsed_df=parsed_df, load_func=self.load)
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


if __name__ == "__main__":
    pass
