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
    """
    Convert numpy types to native Python types for database compatibility.

    Args:
        obj: Object that may contain numpy types (list, array, single value, etc.)

    Returns:
        Object with numpy types converted to native Python types
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
    elif isinstance(obj, np.str_) or isinstance(obj, np.unicode_):
        return str(obj)
    else:
        return obj


def batch_query_existing_keys(conn, table_name, key_column, key_list, batch_size=1000):
    """
    Query existing keys from database in batches to avoid memory issues.

    Args:
        conn: Database connection
        table_name (str): Name of the table to query
        key_column (str): Name of the key column
        key_list (list): List of keys to check
        batch_size (int): Number of keys to process in each batch

    Returns:
        set: Set of existing keys
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
    """Yield mini-batches from an iterator"""
    iterator = iter(iterator)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch


class Logger:
    def __init__(self, logger_level="INFO", log_file=None):
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
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)

    def debug(self, message):
        self.logger.debug(message)

    @staticmethod
    def log_errors(logger, error_type=None, log_success=True):
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
    """Extract browser name from user agent string."""
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
    """Extract operating system from user agent string."""
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
    """Extract domain from referrer URL."""
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
    """
    Returns a dictionary of SQL commands to create fact and dimension tables.

    Returns:
        dict: Dictionary with table names as keys and CREATE TABLE SQL as values.
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
    """
    Returns the PySpark StructType schema for parsing JSON data from Kafka.

    Returns:
        StructType: PySpark schema for Kafka JSON data.
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
    """
    Returns UDFs for Spark transformations.

    Returns:
        dict: Dictionary containing UDFs for extracting browser, OS, and referrer domain.
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
        """
        Get primary key column names for different tables.
        
        Args:
            table_name (str): Name of the table

        Returns:
            list: List of primary key column names
        """
        primary_keys = {
            'fact_sales': ['sales_key'],
            'dim_product': ['product_key'],
            'dim_location': ['ip_key'],
            'dim_user_agent': ['user_agent_key'],
            'dim_date': ['full_date']
        }

        return primary_keys.get(table_name, ['id'])  # Default to 'id' if table not found