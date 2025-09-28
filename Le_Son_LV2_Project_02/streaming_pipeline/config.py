"""Configuration management module for the streaming pipeline.

This module provides utilities for loading configuration from INI files
for various components of the real-time data streaming pipeline.

Author: Son Hai Le
Version: 1.0.0
"""

from configparser import ConfigParser


def load_config(filename='config.ini', section='kafka'):
    """Load configuration from an INI file section.
    
    Reads configuration parameters from a specified section in an INI file
    and returns them as a dictionary. This is commonly used to load database
    connection parameters, Kafka settings, and other service configurations.
    
    Args:
        filename (str, optional): Path to the configuration file. 
            Defaults to 'config.ini'.
        section (str, optional): Section name in the INI file to read from.
            Defaults to 'kafka'.
    
    Returns:
        dict: Dictionary containing all key-value pairs from the specified
            section, with keys and values as strings.
    
    Raises:
        Exception: If the specified section is not found in the configuration file.
        FileNotFoundError: If the configuration file cannot be found.
        configparser.Error: If there are issues parsing the configuration file.
    
    Examples:
        >>> # Load Kafka configuration
        >>> kafka_config = load_config('config.ini', 'kafka')
        >>> print(kafka_config['bootstrap.servers'])
        
        >>> # Load PostgreSQL configuration  
        >>> db_config = load_config('database.ini', 'postgresql')
        >>> print(db_config['host'])
    
    Note:
        The configuration file should follow standard INI format:
        [section_name]
        key1 = value1
        key2 = value2
    """
    parser = ConfigParser()
    parser.read(filename)
    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return config