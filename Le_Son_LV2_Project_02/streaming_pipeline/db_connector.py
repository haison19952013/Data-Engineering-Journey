import re
import os
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from config import load_config
from pyspark.sql import SparkSession
from utils import Logger
import pandas as pd
import utils

logger = Logger()
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.ini")

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
        self.spark_config = load_config(filename=CONFIG_PATH, section=config_name)

    @logger.log_errors(logger)
    def create_spark_session(self):
        """
        Create and configure a Spark session with the loaded configurations.
        
        Returns:
            SparkSession: Configured Spark session instance.
            
        Note:
            The method includes Kafka package for Spark-Kafka integration.
        """
        # Create Spark session
        builder = SparkSession.builder.appName(self.spark_config.get("spark.app.name", "SparkApp")) \
                .master(self.spark_config.get("spark.master", "local[*]")) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            

        # Add additional configurations
        for key, value in self.spark_config.items():
            if key not in ["spark.app.name", "spark.master"]:
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
    def __init__(self, config_name, test_mode= False, insert_batch_size=100_000):
        """
        Initialize PostgresConnector with configuration from specified database section.
        
        Args:
            config_name (str): Section name in config.ini file containing PostgreSQL configuration
        """
        self.db_config = load_config(filename=CONFIG_PATH, section=config_name)
        self.engine = None
        self.test_mode = test_mode
        self.insert_batch_size = insert_batch_size

    @logger.log_errors(logger)
    def get_engine(self):
        """
        Get or create SQLAlchemy engine for PostgreSQL connection.
        
        Returns:
            Engine: SQLAlchemy engine instance.
        """
        if self.engine is None:
            if self.test_mode:
                url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host_out']}:{self.db_config['port_out']}/{self.db_config['database']}"
            else:
                url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host_in']}:{self.db_config['port_in']}/{self.db_config['database']}"
            
            # Optimize connection pool settings for streaming workloads
            self.engine = create_engine(
                url,
                pool_size=10,           # Number of connections to maintain
                max_overflow=20,        # Additional connections allowed beyond pool_size
                pool_timeout=30,        # Timeout to get connection from pool
                pool_recycle=3600,      # Recycle connections every hour
                pool_pre_ping=True,      # Validate connections before use,
                echo=False               # Disable SQL query logging
            )
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
    def insert_record(self, table_name, records, if_exists='append', use_on_conflict=True):
        """
        Insert records into a PostgreSQL table with optional conflict resolution.
        
        Args:
            table_name (str): Name of the table to insert into.
            records (DataFrame or dict or list of dict): Records to insert.
                - If DataFrame: Uses pandas to_sql for efficient bulk insert
                - If dict: Single record
                - If list of dict: Multiple records
            if_exists (str): How to behave if the table exists ('append', 'replace', 'fail')
            use_on_conflict (bool): If True, use ON CONFLICT DO NOTHING to ignore duplicates
                
        Note:
            When use_on_conflict=True, uses custom SQL with ON CONFLICT DO NOTHING
            to gracefully handle duplicate key violations without errors.
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
            if use_on_conflict and if_exists == 'append':
                # Use pandas to_sql with custom method for ON CONFLICT DO NOTHING
                df.to_sql(
                    table_name, 
                    engine, 
                    if_exists=if_exists, 
                    index=False,
                    method=self._get_on_conflict_method(table_name),  # Custom method for conflict resolution
                    chunksize=self.insert_batch_size
                )
            else:
                # Use standard pandas to_sql
                df.to_sql(
                    table_name, 
                    engine, 
                    if_exists=if_exists, 
                    index=False,
                    method='multi',  # Faster bulk inserts
                    chunksize=self.insert_batch_size  # Process in chunks for large datasets
                )
            logger.info(f"Inserted {len(df)} record(s) into {table_name}")
        except Exception as e:
            # Check if it's a duplicate key error
            if "duplicate key value violates unique constraint" in str(e).lower():
                logger.warning(f"Duplicate key detected for {table_name}. This indicates filtering logic needs review.")
            else:
                logger.error(f"Failed to insert records into {table_name}: {e}")
            # Don't raise - just log and continue
    
    def _get_on_conflict_method(self, table_name):
        """
        Returns a method function for pandas to_sql that uses ON CONFLICT DO NOTHING.
        
        Args:
            table_name (str): Name of the target table
            
        Returns:
            function: Method function for pandas to_sql
        """
        def insert_on_conflict_nothing(table, conn, keys, data_iter):
            # Convert data_iter to a list of dictionaries
            data = [dict(zip(keys, row)) for row in data_iter]
            
            # Define primary key columns for each table
            primary_key_columns = utils.get_primary_key_columns(table_name)
            
            # Construct the insert statement with on_conflict_do_nothing
            stmt = insert(table.table).values(data).on_conflict_do_nothing(
                index_elements=primary_key_columns
            )
            
            # Execute the statement
            result = conn.execute(stmt)
            return result.rowcount
        
        return insert_on_conflict_nothing
    
    

if __name__ == "__main__":
    pass