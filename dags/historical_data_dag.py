import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from time import time
from utils import setup_logging
from database import get_file_path_for_period, generate_tables
from etl import extract_monthly_data, process_file
from dotenv import load_dotenv

load_dotenv()

def extract(**kwargs):
    logger = setup_logging()
    logger.info("Starting extracting process")
    
    try:
        logger.info("Generating tables")
        generate_tables(logger)
        
        base_start = datetime(2021, 4, 1)
        run_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d")
        delta_days = (run_date - base_start).days
        start_date = base_start + timedelta(days=delta_days)
        end_date = start_date + timedelta(days=29)

        timestamp = round(time())

        logger.info(f"Extracting data for period: {start_date} to {end_date}")
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(
                extract_monthly_data,
                logger,
                "WEATHER",
                os.getenv("WEATHER_API_URL"),
                start_date,
                end_date,
                timestamp,
            )
            executor.submit(
                extract_monthly_data,
                logger,
                "COVID",
                os.getenv("COVID_API_URL"),
                start_date,
                end_date,
                timestamp,
            )

        logger.info("Extracting process completed successfully")
    except Exception as e:
        logger.error(f"Extracting process failed with error: {str(e)}")
        raise

def transform(**kwargs):
    logger = setup_logging()
    logger.info("Starting transformation process")

    try:
        base_start = datetime(2021, 4, 1)
        run_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d")
        delta_days = (run_date - base_start).days
        start_date = base_start + timedelta(days=delta_days)
        end_date = start_date + timedelta(days=29)

        logger.info(f"Processing transformation for the period: {start_date} to {end_date}")
        
        period_file_paths = get_file_path_for_period(start_date, end_date)
        file_paths = [os.path.join(directory_path, file_name) for directory_path, file_name in period_file_paths]

        for file_path in file_paths:
            process_file(logger, file_path)

        logger.info("Transformation process completed successfully")
    except Exception as e:
        logger.error(f"Transformation process failed with error: {e}")
        raise

with DAG(
    dag_id="historical_data",
    start_date=datetime(2021, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["historical", "weather", "covid", "etl"]
):
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    extract_task >> transform_task