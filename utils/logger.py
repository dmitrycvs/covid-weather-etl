import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging():
    os.makedirs('logs', exist_ok=True)
    
    logger = logging.getLogger('covid_weather_etl')
    logger.setLevel(logging.INFO)
    
    file_handler = RotatingFileHandler(
        'logs/etl_process.log', 
        maxBytes=10*1024*1024,
        backupCount=5
    )
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    return logger