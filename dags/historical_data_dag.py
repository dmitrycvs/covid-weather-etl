import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from time import time
from utils import setup_logging
from database import generate_extract_schema_and_tables, generate_transform_schema_and_tables, generate_load_schema_and_tables, get_file_path_for_period 
from etl import extract_monthly_data, process_file, load_process


def extract(**kwargs):
    logger = setup_logging()
    logger.info("Starting extracting process")
    
    try:
        logger.info("Generating tables")
        generate_extract_schema_and_tables(logger)
        
        start_date = datetime(2021, 4, 1)
        end_date = datetime(2021, 4, 3)
        timestamp = round(time())
        
        logger.info(f"Starting data extraction for period: {start_date} to {end_date}")
        
        extract_monthly_data(logger, "WEATHER", os.environ.get("WEATHER_API_URL"), start_date, end_date, timestamp)
        extract_monthly_data(logger, "COVID", os.environ.get("COVID_API_URL"), start_date, end_date, timestamp)
        
        logger.info("Extracting process completed successfully")
        return "Extraction completed successfully"
    except Exception as e:
        logger.error(f"Extracting process failed with error: {str(e)}")
        raise

def transform(**kwargs):
    logger = setup_logging()
    logger.info("Starting transformation process")

    generate_transform_schema_and_tables(logger)

    try:
        start_date = datetime(2021, 4, 1)
        end_date = datetime(2021, 4, 3)
        period_data = get_file_path_for_period(start_date, end_date)

        for record in period_data:
            import_id = record[0]
            country_id = record[1]
            directory_name = record[2]
            file_name = record[3]
            file_path = os.path.join("/opt/airflow", directory_name, file_name)
            process_file(logger, import_id, country_id, file_path)
            
        logger.info("Transformation process completed successfully")
    except Exception as e:
        logger.error(f"Transformation process failed with error: {e}")
        raise

def load(**kwargs):
    try:
        logger = setup_logging()
        logger.info("Starting load process")
        generate_load_schema_and_tables(logger)
        start_date = datetime(2021, 4, 1)
        end_date = datetime(2021, 4, 3)

        period_data = get_file_path_for_period(start_date, end_date, processed=True)

        for record in period_data:
            id = record[0]
            directory_name = record[1]
            file_name = record[2]
            file_path = os.path.join("/opt/airflow", directory_name, file_name)
            load_process(logger, id, file_path)
        
        logger.info("Load process completed successfully")
    except Exception as e:
        logger.error(f"Load process failed with error: {e}")
        raise

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='etl',
    default_args=default_args,
    description='DAG which does the ETL process',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False, 
    tags=['historical', 'backfill', 'etl'],
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task