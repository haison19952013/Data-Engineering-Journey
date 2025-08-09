from bs4 import BeautifulSoup
import re
from underthesea import sent_tokenize
import logging
import os
import time
import sys
from itertools import islice
from collections import defaultdict
from typing import Any
import os
import json
import fastavro


def clean_text(text):
    def clean(token):
        token = token.lower()
        token = token.replace('\r', ' ')
        token = token.replace('\t', ' ')
        token = re.sub(r'\n', ' ', token)
        token = re.sub(r'\.{2,}', ' ', token)
        punctuation = re.compile(r'[{};():,."/<>-]')
        token = punctuation.sub(' ', token)
        token = token.strip()
        token = re.sub(r'\s{2,}', ' ', token)
        return token
    text = BeautifulSoup(text, 'html.parser').get_text()
    tokens = sent_tokenize(text)
    tokens = list(map(clean, tokens))
    tokens = ' '.join(tokens)
    return tokens

def batch_iterator(iterator, batch_size):
    """Yield mini-batches from an iterator"""
    iterator = iter(iterator)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch

class Logger:
    def __init__(self, logger_level = "INFO",log_file=None):
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
            
            file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
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
    def log_errors(logger, error_type = None, log_success = True):
        if error_type is None:
            error_type = Exception
        def decorator(func):
            """Decorator for logging errors in function execution"""
            def wrapper(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    if log_success:
                        args_str = ', '.join([f'{arg}={value}' for arg, value in kwargs.items()])
                        info_message = f'Success in {func.__name__}({args_str})'
                        logger.info(info_message)
                    return result
                except error_type as e:
                    args_str = ', '.join([f'{arg}={value}' for arg, value in kwargs.items()])
                    error_message = f'Error in {func.__name__}({args_str}): {str(e)}'
                    logger.error(error_message)
            return wrapper
        return decorator

  
    @staticmethod
    def log_timestamp(logger):
        def decorator(func):
            """Decorator for logging execution time of a function"""
            def wrapper(*args, **kwargs):
                logger.info(f'Starting {func.__name__} at {time.strftime("%Y-%m-%d %H:%M:%S")}')
                start_time = time.time()
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f'Ending {func.__name__} at {time.strftime("%Y-%m-%d %H:%M:%S")}')
                logger.info(f"{func.__name__} completed in {duration:.2e} seconds")
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

import re

def clean_number(price):
    try:
        if isinstance(price, str):
            price = re.sub(r'[^\d.]', '', price)
        if price.count('.') > 1:
            parts = price.split('.')
            # Join all parts except the last as integer part
            price = ''.join(parts[:-1]) + '.' + parts[-1]
        return float(price)
    except:
        return None

def normalize_record(record):
    # Normalize cart_products
    cart_products = record.get("cart_products")
    if isinstance(cart_products, list):
        for idx, cart_product in enumerate(cart_products):
            option = cart_product.get("option")
            if option == "":
                cart_product["option"] = []
            elif isinstance(option, dict):
                cart_product["option"] = [option]

            # price = cart_product.get("price")
            # if isinstance(price, str):
            #     cart_product["price"] = clean_number(price)

    # Normalize option
    option = record.get("option")
    if option == "":
        record["option"] = []
    elif isinstance(option, dict):
        record["option"] = [option]

    # Normalize price
    # price = record.get("price")
    # if isinstance(price, str):
    #     record["price"] = clean_number(price)

    # Normalize empty strings to None
    for key, value in record.items():
        if value == "":
            record[key] = None
    order_id = record.get("order_id")
    if order_id is not None:
        record["order_id"] = str(order_id)

    return record


def extract_schema_from_mongodb(collection, output_folder, explore_limit = 20_000):
    
    combined_schema = defaultdict(set)
    # Step 1: Get all distinct collection types
    collection_types = collection.distinct("collection")
    # Step 2: Loop and analyze each collection type
    for coll_type in collection_types:
        print(f"\n🔍 Analyzing: {coll_type}")
        field_types = defaultdict(set)

        # Sample up to 20k documents per type
        sample_cursor = collection.find({"collection": coll_type}).limit(explore_limit)
        for doc in sample_cursor:
            doc_types = extract_types(normalize_record(doc))
            for path, types in doc_types.items():
                field_types[path].update(types)
                combined_schema[path].update(types)

        # Save individual schema
        with open(os.path.join(output_folder, f"{coll_type}_schema.json"), "w") as f:
            json.dump({k: sorted(list(v)) for k, v in field_types.items()}, f, indent=2)

        print(f"✅ Saved: {coll_type}_schema.json ({len(field_types)} fields)")
    
    combined_path = "combined_schema.json"
    with open(os.path.join(output_folder, combined_path), "w") as f:
        json.dump({k: sorted(list(v)) for k, v in combined_schema.items()}, f, indent=2)

    print(f"\n📦 Combined schema saved to '{os.path.join(output_folder, combined_path)}' with {len(combined_schema)} unique fields.")    

def convert_type_to_avro(type_list, make_nullable=True):
    """Convert Python types to Avro types"""
    avro_types = []
    
    # Always add null first if making nullable
    if make_nullable:
        avro_types.append("null")
    
    for py_type in type_list:
        if py_type == "str":
            avro_types.append("string")
        elif py_type == "int":
            avro_types.append("int")
        elif py_type == "float":
            avro_types.append("double")
        elif py_type == "bool":
            avro_types.append("boolean")
        elif py_type == "NoneType":
            # Skip adding null again if we already added it
            if not make_nullable:
                avro_types.append("null")
        elif py_type == "dict":
            avro_types.append({
                "type": "map",
                "values": ["null", "string"] if make_nullable else "string"
            })
        elif py_type == "list":
            avro_types.append({
                "type": "array",
                "items": ["null", "string"] if make_nullable else "string"
            })
        elif py_type == "ObjectId":
            avro_types.append("string")  # MongoDB ObjectId as string
    
    # Remove duplicates while preserving order
    seen = set()
    unique_types = []
    for t in avro_types:
        if isinstance(t, dict):
            # For complex types, convert to string for comparison
            t_str = str(t)
            if t_str not in seen:
                seen.add(t_str)
                unique_types.append(t)
        else:
            if t not in seen:
                seen.add(t)
                unique_types.append(t)
    
    # If only null, return ["null", "string"] as fallback
    if len(unique_types) == 1 and unique_types[0] == "null":
        return ["null", "string"]
    
    # If only one non-null type, still return as union if nullable
    if len(unique_types) == 2 and unique_types[0] == "null":
        return unique_types
    
    # Return union for multiple types
    return unique_types if len(unique_types) > 1 else unique_types[0]

def get_default_value(avro_type):
    """Get appropriate default value for Avro type - always null for nullable fields"""
    if isinstance(avro_type, list):
        # Union type - always default to null if present
        if "null" in avro_type:
            return None
        else:
            # Get default for first type
            first_type = avro_type[0]
            return get_default_value(first_type)
    elif isinstance(avro_type, dict):
        if avro_type.get("type") == "array":
            return None  # Default to null for arrays too
        elif avro_type.get("type") == "map":
            return None  # Default to null for maps too
        elif avro_type.get("type") == "record":
            return None  # Records default to null
        else:
            return None
    elif avro_type == "string":
        return None  # Default to null instead of empty string
    elif avro_type == "int":
        return None  # Default to null instead of 0
    elif avro_type == "long":
        return None  # Default to null instead of 0
    elif avro_type == "double":
        return None  # Default to null instead of 0.0
    elif avro_type == "float":
        return None  # Default to null instead of 0.0
    elif avro_type == "boolean":
        return None  # Default to null instead of false
    elif avro_type == "null":
        return None
    else:
        return None

def create_nested_record(field_name, nested_fields, make_nullable=True):
    """Create a nested record type for complex objects"""
    record_fields = []
    
    for nested_field, types in nested_fields.items():
        # Remove the parent field name prefix
        clean_field_name = nested_field.replace(f"{field_name}.", "")
        
        field_type = convert_type_to_avro(types, make_nullable=make_nullable)
        field_def = {
            "name": clean_field_name,
            "type": field_type,
            "default": None  # Always default to null
        }
        
        record_fields.append(field_def)
    
    return {
        "type": "record",
        "name": field_name.replace(".", "_") + "_record",
        "fields": record_fields
    }

def flat_json_to_avro_schema(schema_dict, make_all_nullable=True):
    """Convert JSON schema to Avro schema with all fields nullable"""
    
    # Initialize Avro schema
    avro_schema = {
        "type": "record",
        "name": "EventRecord",
        "fields": []
    }
    
    # Track nested fields
    nested_fields = {}
    processed_fields = set()
    
    # First pass: identify nested fields
    for field_name, types in schema_dict.items():
        if "." in field_name:
            parent_field = field_name.split(".")[0]
            if parent_field not in nested_fields:
                nested_fields[parent_field] = {}
            nested_fields[parent_field][field_name] = types
    
    # Second pass: process all fields
    for field_name, types in schema_dict.items():
        if field_name in processed_fields:
            continue
            
        if "." not in field_name:
            # Handle top-level fields
            if field_name in nested_fields:
                # This field has nested properties
                field_types = []
                
                # Always add null first
                if make_all_nullable:
                    field_types.append("null")
                
                # Check if it's an array or object field
                if "list" in types:
                    # Create array of records
                    nested_record = create_nested_record(field_name, nested_fields[field_name], make_all_nullable)
                    field_types.append({
                        "type": "array",
                        "items": nested_record
                    })
                
                if "dict" in types:
                    # Create map
                    field_types.append({
                        "type": "map",
                        "values": ["null", "string"] if make_all_nullable else "string"
                    })
                
                # Add other base types
                base_types = convert_type_to_avro(types, make_nullable=False)
                if isinstance(base_types, list):
                    for bt in base_types:
                        if bt != "null" and bt not in field_types:
                            field_types.append(bt)
                else:
                    if base_types != "null" and base_types not in field_types:
                        field_types.append(base_types)
                
                field_type = field_types if len(field_types) > 1 else field_types[0]
                
                # Mark nested fields as processed
                for nested_field in nested_fields[field_name]:
                    processed_fields.add(nested_field)
            else:
                # Simple field
                field_type = convert_type_to_avro(types, make_nullable=make_all_nullable)
            
            avro_field = {
                "name": field_name,
                "type": field_type,
                "default": None  # Always default to null
            }
            
            avro_schema["fields"].append(avro_field)
            processed_fields.add(field_name)
    
    return fastavro.parse_schema(avro_schema)