"""Utility functions and classes for the streaming pipeline.

This module provides common utility functions for data processing, logging,
type conversions, schema definitions, and user-defined functions (UDFs) used
throughout the real-time streaming pipeline.

Key Components:
    - Data type conversion utilities
    - Database query helpers
    - Logging infrastructure
    - Spark schema definitions
    - User-defined functions for data transformation
    - Table creation SQL generators

Author: Son Hai Le
Version: 1.0.0
"""

import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from itertools import islice
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import numpy as np
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    LongType,
    ArrayType,
    MapType,
)
from pyspark.sql.functions import udf
from sqlalchemy import text


def convert_numpy_types(obj):
    """Convert numpy types to native Python types for database compatibility.
    
    Recursively converts numpy data types within complex objects (lists, dicts, arrays)
    to native Python types to ensure compatibility with database operations and
    JSON serialization.

    Args:
        obj: Object that may contain numpy types. Can be a single value, list,
            tuple, dict, or numpy array containing numpy types.

    Returns:
        Object with all numpy types converted to equivalent native Python types:
            - np.integer → int
            - np.floating → float  
            - np.bool_ → bool
            - np.str_ → str
            - np.ndarray → list
            - Complex structures are recursively processed

    Examples:
        >>> import numpy as np
        >>> data = {'values': np.array([1, 2, 3]), 'flag': np.bool_(True)}
        >>> convert_numpy_types(data)
        {'values': [1, 2, 3], 'flag': True}
        
        >>> convert_numpy_types(np.int64(42))
        42
    """
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_types(item) for item in obj)
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.str_):
        return str(obj)
    else:
        return obj


def batch_query_existing_keys(conn, table_name, key_column, key_list, batch_size=1000):
    """Query existing keys from database in batches to avoid memory issues.
    
    Performs batch queries to check which keys already exist in a database table.
    This approach prevents memory issues when checking large numbers of keys
    and avoids database query limits.

    Args:
        conn: Database connection object (SQLAlchemy connection).
        table_name (str): Name of the database table to query.
        key_column (str): Name of the column containing the keys to check.
        key_list (list): List of key values to check for existence.
        batch_size (int, optional): Maximum number of keys to process in each
            batch query. Defaults to 1000.

    Returns:
        set: Set containing all keys that exist in the database table.

    Examples:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine("postgresql://...")
        >>> with engine.connect() as conn:
        ...     existing = batch_query_existing_keys(
        ...         conn, 'products', 'product_id', [1, 2, 3, 4, 5]
        ...     )
        ...     print(existing)
        {1, 3, 5}  # Keys 1, 3, 5 exist in database

    Note:
        Uses SQL ANY operator for efficient batch querying. The function
        handles empty batches gracefully and accumulates results across
        all batches.
    """
    existing_keys = set()

    # Process in batches
    for i in range(0, len(key_list), batch_size):
        batch = key_list[i:i + batch_size]
        if batch:
            query = text(
                f"SELECT {key_column} FROM {table_name} WHERE {key_column} = ANY(:{key_column}_list)")
            result = conn.execute(query, {f"{key_column}_list": batch})
            batch_existing = {row[0] for row in result.fetchall()}
            existing_keys.update(batch_existing)

    return existing_keys


def batch_iterator(iterator, batch_size):
    """Yield mini-batches from an iterator.
    
    Splits an iterator into smaller batches of the specified size. Useful
    for processing large datasets in manageable chunks to control memory
    usage and processing performance.

    Args:
        iterator: Any iterable object (list, generator, etc.).
        batch_size (int): Maximum number of items per batch.

    Yields:
        list: List containing up to batch_size items from the iterator.

    Examples:
        >>> data = range(10)
        >>> for batch in batch_iterator(data, 3):
        ...     print(batch)
        [0, 1, 2]
        [3, 4, 5] 
        [6, 7, 8]
        [9]

        >>> # Process large dataset in batches
        >>> for batch in batch_iterator(large_dataset, 1000):
        ...     process_batch(batch)
    """
    iterator = iter(iterator)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch


class Logger:
    """Enhanced logging utility for the streaming pipeline.
    
    Provides structured logging with both console and file output capabilities,
    along with decorators for error handling and performance monitoring.
    
    Attributes:
        logger (logging.Logger): The underlying Python logger instance.
        
    Examples:
        >>> logger = Logger(logger_level="INFO", log_file="pipeline.log")
        >>> logger.info("Processing started")
        >>> logger.error("An error occurred")
        
        >>> # Using as a decorator
        >>> @logger.log_errors(logger)
        ... def process_data():
        ...     return "success"
    """
    
    def __init__(self, logger_level="INFO", log_file=None):
        """Initialize the Logger with specified level and optional file output.
        
        Args:
            logger_level (str, optional): Logging level (DEBUG, INFO, WARNING, ERROR).
                Defaults to "INFO".
            log_file (str, optional): Path to log file. If provided, logs will be
                written to both console and file. Defaults to None (console only).
                
        Note:
            The log file directory will be created automatically if it doesn't exist.
            Existing handlers are cleared to avoid duplicate logging.
        """
        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logger_level)

        # Clear any existing handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(
            "{asctime} - {levelname} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M"
        )

        # Always add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # Add file handler if log_file is specified
        if log_file:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(log_file), exist_ok=True)

            file_handler = logging.FileHandler(
                log_file, mode='w', encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def info(self, message):
        """Log an info level message.
        
        Args:
            message (str): The message to log.
        """
        self.logger.info(message)

    def error(self, message):
        """Log an error level message.
        
        Args:
            message (str): The error message to log.
        """
        self.logger.error(message)

    def warning(self, message):
        """Log a warning level message.
        
        Args:
            message (str): The warning message to log.
        """
        self.logger.warning(message)

    def debug(self, message):
        """Log a debug level message.
        
        Args:
            message (str): The debug message to log.
        """
        self.logger.debug(message)

    @staticmethod
    def log_errors(logger, error_type=None, log_success=True):
        """Decorator for automatic error logging and handling.
        
        Creates a decorator that catches exceptions in function execution
        and logs them automatically, with optional success logging.
        
        Args:
            logger: Logger instance to use for logging.
            error_type (Exception, optional): Specific exception type to catch.
                Defaults to None (catches all exceptions).
            log_success (bool, optional): Whether to log successful executions.
                Defaults to True.
                
        Returns:
            function: Decorator function that can be applied to other functions.
            
        Examples:
            >>> @Logger.log_errors(logger)
            ... def risky_function():
            ...     return 1 / 0  # This will be caught and logged
        """
        if error_type is None:
            error_type = Exception

        def decorator(func):
            """Decorator for logging errors in function execution"""
            def wrapper(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    if log_success:
                        args_str = ', '.join(
                            [f'{arg}={value}' for arg, value in kwargs.items()])
                        info_message = f'Success in {func.__name__}({args_str})'
                        logger.info(info_message)
                    return result
                except error_type as e:
                    args_str = ', '.join(
                        [f'{arg}={value}' for arg, value in kwargs.items()])
                    error_message = f'Error in {func.__name__}({args_str}): {str(e)}'
                    logger.error(error_message)
            return wrapper
        return decorator

    @staticmethod
    def log_timestamp(logger):
        """Decorator for logging function execution time and timestamps.
        
        Creates a decorator that logs start time, end time, and duration
        of function execution for performance monitoring.
        
        Args:
            logger: Logger instance to use for logging.
            
        Returns:
            function: Decorator function that can be applied to other functions.
            
        Examples:
            >>> @Logger.log_timestamp(logger)
            ... def slow_function():
            ...     time.sleep(2)
            ...     return "done"
        """
        def decorator(func):
            """Decorator for logging execution time of a function"""
            def wrapper(*args, **kwargs):
                logger.info(
                    f'Starting {func.__name__} at {time.strftime("%Y-%m-%d %H:%M:%S")}')
                start_time = time.time()
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(
                    f'Ending {func.__name__} at {time.strftime("%Y-%m-%d %H:%M:%S")}')
                logger.info(
                    f"{func.__name__} completed in {duration:.2e} seconds")
                return result
            return wrapper
        return decorator


def extract_types(doc: Any, parent_key=''):
    """Extract and catalog data types from nested JSON-like structures.
    
    Recursively analyzes nested dictionaries and lists to identify all
    data types present in the structure. Useful for schema inference
    and data validation.
    
    Args:
        doc (Any): The document/object to analyze (dict, list, or primitive).
        parent_key (str, optional): Key path prefix for nested structures.
            Defaults to empty string.
    
    Returns:
        dict: Dictionary mapping key paths to sets of data types found
            at those paths.
            
    Examples:
        >>> data = {
        ...     'name': 'John',
        ...     'age': 30,
        ...     'scores': [85, 90, 78],
        ...     'profile': {'active': True}
        ... }
        >>> types = extract_types(data)
        >>> print(types)
        {'name': {'str'}, 'age': {'int'}, 'scores': {'int'}, 'profile.active': {'bool'}}
    """
    types = defaultdict(set)
    if isinstance(doc, dict):
        for k, v in doc.items():
            full_key = f"{parent_key}.{k}" if parent_key else k
            types[full_key].add(type(v).__name__)
            nested_types = extract_types(v, full_key)
            for nk, tv in nested_types.items():
                types[nk].update(tv)
    elif isinstance(doc, list):
        for item in doc:
            nested_types = extract_types(item, parent_key)
            for nk, tv in nested_types.items():
                types[nk].update(tv)
    return types


def extract_browser(user_agent):
    """Extract browser name from user agent string.
    
    Parses user agent strings to identify the browser name using
    regular expressions. Handles major browsers including Chrome,
    Firefox, Safari, Edge, and Internet Explorer.
    
    Args:
        user_agent (str): The user agent string from HTTP headers.
    
    Returns:
        str: Identified browser name or 'Unknown' if not recognized.
        Returns 'Unknown' for None or NaN inputs.
    
    Examples:
        >>> ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        >>> extract_browser(ua)
        'Chrome'
        
        >>> extract_browser("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0")
        'Firefox'
        
        >>> extract_browser(None)
        'Unknown'
    """
    if not user_agent or pd.isna(user_agent):
        return None

    ua = user_agent.lower()

    if 'chrome' in ua and 'safari' in ua:
        if 'edg' in ua:
            return 'Edge'
        elif 'opr' in ua or 'opera' in ua:
            return 'Opera'
        else:
            return 'Chrome'
    elif 'firefox' in ua:
        return 'Firefox'
    elif 'safari' in ua and 'chrome' not in ua:
        return 'Safari'
    elif 'edg' in ua:
        return 'Edge'
    elif 'opera' in ua or 'opr' in ua:
        return 'Opera'
    elif 'msie' in ua or 'trident' in ua:
        return 'Internet Explorer'
    else:
        return 'Other'


def extract_os(user_agent):
    """Extract operating system from user agent string.
    
    Parses user agent strings to identify the operating system using
    keyword matching. Handles major OS families including Windows,
    macOS, Linux, Android, iOS, and Chrome OS.
    
    Args:
        user_agent (str): The user agent string from HTTP headers.
    
    Returns:
        str: Identified operating system name or 'Other' if not recognized.
        Returns None for None or NaN inputs.
    
    Examples:
        >>> ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        >>> extract_os(ua)
        'Windows'
        
        >>> extract_os("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        'macOS'
        
        >>> extract_os("Mozilla/5.0 (X11; Linux x86_64)")
        'Linux'
    """
    if not user_agent or pd.isna(user_agent):
        return None

    ua = user_agent.lower()

    if 'windows' in ua:
        return 'Windows'
    elif 'mac os x' in ua or 'macos' in ua:
        return 'macOS'
    elif 'linux' in ua:
        return 'Linux'
    elif 'android' in ua:
        return 'Android'
    elif 'iphone' in ua or 'ipad' in ua or 'ipod' in ua:
        return 'iOS'
    elif 'cros' in ua:
        return 'Chrome OS'
    else:
        return 'Other'


def extract_referrer_domain(referrer_url):
    """Extract domain from referrer URL.
    
    Parses referrer URLs to extract the domain name. Handles URLs
    with or without protocol schemes and returns the network location
    (domain) portion.
    
    Args:
        referrer_url (str): The referrer URL from HTTP headers.
    
    Returns:
        str: The domain name extracted from the URL, or None if
        the URL is invalid, empty, or cannot be parsed.
    
    Examples:
        >>> extract_referrer_domain("https://www.google.com/search?q=test")
        'www.google.com'
        
        >>> extract_referrer_domain("facebook.com")
        'facebook.com'
        
        >>> extract_referrer_domain("")
        None
        
        >>> extract_referrer_domain(None)
        None
    
    Note:
        If the URL doesn't contain a protocol (http/https), the function
        automatically adds 'http://' prefix for proper parsing.
    """
    if not referrer_url or pd.isna(referrer_url) or referrer_url.strip() == "":
        return None

    try:
        # If URL doesn't contain protocol, add http://
        if "//" not in referrer_url:
            parsed = urlparse("http://" + referrer_url)
        else:
            parsed = urlparse(referrer_url)

        return parsed.netloc if parsed.netloc else None
    except Exception:
        return None


def get_table_creation_sql():
    """Get SQL commands for creating all database tables.
    
    Returns a dictionary containing CREATE TABLE SQL commands for all
    fact and dimension tables in the data warehouse schema.
    
    Returns:
        dict: Dictionary mapping table names to their CREATE TABLE SQL
        commands. Includes fact_sales table and dimension tables for
        date, product, location, and user_agent.
        
    Examples:
        >>> sql_commands = get_table_creation_sql()
        >>> print(sql_commands['fact_sales'])  # Returns CREATE TABLE SQL
        >>> for table_name, sql in sql_commands.items():
        ...     conn.execute(sql)  # Create all tables
        
    Note:
        All tables use 'CREATE TABLE IF NOT EXISTS' to avoid errors
        when tables already exist. Primary keys are defined for all
        dimension tables and the fact table.
    """
    sql_commands = {}

    # Fact table: fact_sales
    sql_commands['fact_sales'] = """
    CREATE TABLE IF NOT EXISTS fact_sales (
        sales_key VARCHAR PRIMARY KEY,
        full_date VARCHAR,
        full_time VARCHAR,
        ip_key VARCHAR,
        user_agent_key VARCHAR,
        product_key VARCHAR,
        referrer_url VARCHAR,
        collection VARCHAR,
        option TEXT,
        email_address VARCHAR,
        resolution VARCHAR,
        user_id_db VARCHAR,
        device_id VARCHAR,
        api_version VARCHAR,
        store_id VARCHAR,
        local_time VARCHAR,
        show_recommendation VARCHAR,
        recommendation TEXT,
        utm_source VARCHAR,
        utm_medium VARCHAR
    );
    """

    # Dimension table: dim_date
    sql_commands['dim_date'] = """
    CREATE TABLE IF NOT EXISTS dim_date (
        full_date VARCHAR PRIMARY KEY,
        day_of_week INTEGER NOT NULL,
        day_of_week_short VARCHAR NOT NULL,
        is_weekday_or_weekend VARCHAR NOT NULL,
        day_of_month INTEGER NOT NULL,
        year_month VARCHAR NOT NULL,
        month INTEGER NOT NULL,
        day_of_year INTEGER NOT NULL,
        week_of_year INTEGER NOT NULL,
        quarter_number INTEGER NOT NULL,
        year INTEGER NOT NULL
    );
    """

    # Dimension table: dim_product
    sql_commands['dim_product'] = """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key VARCHAR PRIMARY KEY,
        name VARCHAR,
        current_url VARCHAR,
        language_code VARCHAR
    );
    """

    # Dimension table: dim_location
    sql_commands['dim_location'] = """
    CREATE TABLE IF NOT EXISTS dim_location (
        ip_key VARCHAR PRIMARY KEY,
        ip_address VARCHAR,
        country_code VARCHAR,
        country_name VARCHAR,
        region_name VARCHAR,
        city_name VARCHAR
    );
    """

    # Dimension table: dim_user_agent
    sql_commands['dim_user_agent'] = """
    CREATE TABLE IF NOT EXISTS dim_user_agent (
        user_agent_key VARCHAR PRIMARY KEY,
        user_agent TEXT,
        browser VARCHAR,
        os VARCHAR
    );
    """

    return sql_commands


def get_kafka_json_schema():
    """Get PySpark StructType schema for parsing Kafka JSON messages.
    
    Returns a predefined Spark schema for parsing JSON data consumed
    from Kafka topics. This schema defines the structure and data types
    for all fields in the streaming data.
    
    Returns:
        StructType: PySpark schema object defining the structure of
        the JSON messages from Kafka, including all field names and
        their corresponding data types.
        
    Examples:
        >>> schema = get_kafka_json_schema()
        >>> df = spark.readStream.format("kafka").load()
        >>> parsed_df = df.select(from_json(col("value"), schema).alias("data"))
        
    Note:
        The schema includes fields for tracking data (_id, time_stamp),
        user information (ip, user_agent, user_id_db), product data
        (product_id, option), and marketing attribution (utm_source, utm_medium).
    """
    return StructType(
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
            StructField("option", ArrayType(
                MapType(StringType(), StringType()))),
            StructField("id", StringType()),
        ]
    )

def get_udfs(udf_name: str):
    """Get Spark User Defined Functions (UDFs) for data transformation.
    
    Returns specific UDF instances for common data transformation tasks
    in the streaming pipeline, such as parsing user agents and URLs.
    
    Args:
        udf_name (str): Name of the UDF to retrieve. Valid options are:
            - 'extract_browser': Extract browser name from user agent
            - 'extract_os': Extract OS name from user agent  
            - 'extract_referrer_domain': Extract domain from referrer URL
    
    Returns:
        pyspark.sql.functions.udf: The requested UDF function wrapped
        for use in Spark DataFrame transformations.
    
    Raises:
        ValueError: If the requested UDF name is not recognized.
        
    Examples:
        >>> extract_browser_udf = get_udfs('extract_browser')
        >>> df = df.withColumn('browser', extract_browser_udf(col('user_agent')))
        
        >>> extract_os_udf = get_udfs('extract_os')
        >>> df = df.withColumn('os', extract_os_udf(col('user_agent')))
    """
    if udf_name == 'extract_browser':
        return udf(extract_browser, StringType())
    elif udf_name == 'extract_os':
        return udf(extract_os, StringType())
    elif udf_name == 'extract_referrer_domain':
        return udf(extract_referrer_domain, StringType())
    else:
        raise ValueError(f"Unknown UDF name: {udf_name}")

def get_primary_key_columns(table_name):
    """Get primary key column names for database tables.
    
    Returns the primary key column name(s) for different tables
    in the data warehouse schema. Used for upsert operations
    and data integrity checks.
    
    Args:
        table_name (str): Name of the database table.
    
    Returns:
        str: Primary key column name for the specified table.
    
    Raises:
        ValueError: If the table name is not recognized.
        
    Examples:
        >>> pk = get_primary_key_columns('fact_sales')
        >>> print(pk)
        'sales_key'
        
        >>> pk = get_primary_key_columns('dim_product')
        >>> print(pk)
        'product_key'
    """
    primary_keys = {
        'fact_sales': ['sales_key'],
        'dim_product': ['product_key'],
        'dim_location': ['ip_key'],
        'dim_user_agent': ['user_agent_key'],
        'dim_date': ['full_date']
    }

    return primary_keys.get(table_name, ['id'])  # Default to 'id' if table not found