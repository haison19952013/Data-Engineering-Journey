from bs4 import BeautifulSoup
import re
from underthesea import sent_tokenize
import logging
import os
import time
import sys

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

class Logger:
    def __init__(self, log_file=None):
        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
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
    def log_errors(logger):
        def decorator(func):
            """Decorator for logging errors in function execution"""
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
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
        