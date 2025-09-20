import re

from sqlalchemy import create_engine, text
from config import load_config
from pyspark.sql import SparkSession
from utils import Logger
import pandas as pd

logger = Logger()

class SparkConnector:
    """
    A connector class for managing Spark session creation and configuration.
    
    This class provides methods to create a Spark session with specified configurations,
    including support for Kafka integration.
    
    Attributes:
        spark_config (dict): Configuration dictionary loaded from database.ini file
        
    Example:
        >>> spark_conn = SparkConnector("local_spark")
        >>> spark = spark_conn.create_spark_session()
    """
    def __init__(self, config_name):
        """
        Initialize SparkConnector with configuration from specified database section.
        
        Args:
            config_name (str): Section name in config .ini file containing Spark configuration
        """
        self.spark_config = load_config(filename="config.ini", section=config_name)

    @logger.log_errors(logger)
    def create_spark_session(self):
        """
        Create and configure a Spark session with the loaded configurations.
        
        Returns:
            SparkSession: Configured Spark session instance.
            
        Note:
            The method includes Kafka package for Spark-Kafka integration.
        """
        
        # Build Spark session with configurations
        builder = SparkSession.builder.appName(self.spark_config.get("app_name", "SparkApp")) \
                .master(self.spark_config.get("master", "local[*]")) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            

        # Add additional configurations
        for key, value in self.spark_config.items():
            if key not in ["app_name", "master"]:
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        return spark
    
class PostgresConnector:
    """
    A connector class for managing PostgreSQL database connections.
    
    This class provides methods to establish connections to a PostgreSQL database
    using configurations loaded from a database.ini file.
    
    Attributes:
        db_config (dict): Configuration dictionary loaded from database.ini file
        
    Example:
        >>> pg_conn = PostgresConnector("docker_postgres")
        >>> conn = pg_conn.conn_postgres()
    """
    def __init__(self, config_name, test= False, insert_batch_size=100_000):
        """
        Initialize PostgresConnector with configuration from specified database section.
        
        Args:
            config_name (str): Section name in config.ini file containing PostgreSQL configuration
        """
        self.db_config = load_config(filename="config.ini", section=config_name)
        self.engine = None
        self.test = test
        self.insert_batch_size = insert_batch_size

    @logger.log_errors(logger)
    def get_engine(self):
        """
        Get or create SQLAlchemy engine for PostgreSQL connection.
        
        Returns:
            Engine: SQLAlchemy engine instance.
        """
        if self.engine is None:
            if self.test:
                url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host_out']}:{self.db_config['port_out']}/{self.db_config['database']}"
            else:
                url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host_in']}:{self.db_config['port_in']}/{self.db_config['database']}"
            self.engine = create_engine(url)
        return self.engine
    
    @logger.log_errors(logger)
    def create_table(self, create_table_sql):
        """
        Create a table in the PostgreSQL database using the provided SQL statement,
        but only if the table does not already exist.
        
        Args:
            create_table_sql (str): SQL statement to create the table.
            
        Note:
            This method checks if the table exists before attempting to create it.
            If the table exists, it logs a message and skips creation.
        """
        # Extract table name from SQL
        match = re.search(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)', create_table_sql, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE TABLE SQL: unable to extract table name")
        table_name = match.group(1)
        
        engine = self.get_engine()
        
        # Check if table exists
        with engine.connect() as conn:
            result = conn.execute(text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = :table_name)"), {"table_name": table_name})
            exists = result.fetchone()[0]
            
            if exists:
                logger.info(f"Table '{table_name}' already exists. Skipping creation.")
            else:
                conn.execute(text(create_table_sql))
                conn.commit()
                logger.info(f"Table '{table_name}' created successfully.")

    @logger.log_errors(logger)
    def insert_record(self, table_name, records):
        """
        Insert records into a PostgreSQL table.
        
        Args:
            table_name (str): Name of the table to insert into.
            records (DataFrame or dict or list of dict): Records to insert.
                - If DataFrame: Uses pandas to_sql for efficient bulk insert
                - If dict: Single record
                - If list of dict: Multiple records
                
        Note:
            For DataFrame input, uses pandas to_sql with if_exists='append'.
            For dict/list, converts to DataFrame first.
        """
        engine = self.get_engine()
        
        if isinstance(records, pd.DataFrame):
            df = records
        elif isinstance(records, dict):
            df = pd.DataFrame([records])
        elif isinstance(records, list) and records:
            df = pd.DataFrame(records)
        else:
            return
        
        if df.empty:
            return
        
        try:
            df.to_sql(
                table_name, 
                engine, 
                if_exists='append', 
                index=False,
                method='multi',  # Faster bulk inserts
                chunksize=self.insert_batch_size  # Process in chunks for large datasets
            )
            logger.info(f"Inserted {len(df)} record(s) into {table_name}")
        except Exception as e:
            # Check if it's a duplicate key error
            if "duplicate key value violates unique constraint" in str(e).lower():
                logger.warning(f"Duplicate key detected for {table_name}. This indicates filtering logic needs review.")
            logger.error(f"Failed to insert records into {table_name}: {e}")
            raise

if __name__ == "__main__":
    pass