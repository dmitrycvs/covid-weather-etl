import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta
from time import time

from airflow.decorators import dag, task
from airflow.models import Variable

from etl.extractor import Extractor
from etl.transformer import Transformer
from etl.loader import Loader
from utils import setup_logging

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="etl",
    default_args=default_args,
    description="DAG which does the ETL process",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["historical", "backfill", "etl"],
)
def etl_dag():
    @task
    def get_start_date():
        start_date_str = Variable.get("start_date", default_var=None)
        return datetime.fromisoformat(start_date_str) if start_date_str else datetime(2021, 4, 1)

    @task
    def calculate_end_date(start_date):
        return start_date + timedelta(days=2)

    @task
    def set_start_date(date):
        Variable.set("start_date", date.isoformat())

    @task
    def extract(start_date, end_date, logger):
        timestamp = round(time())
        covid_extractor = Extractor("COVID", os.getenv("COVID_API_URL"), start_date, end_date, timestamp, logger)
        covid_extractor.run()
        
        weather_extractor = Extractor("WEATHER", os.getenv("WEATHER_API_URL"), start_date, end_date, timestamp, logger)
        weather_extractor.run()
    @task 
    def transform(start_date, end_date, logger):
        transformer = Transformer(start_date, end_date, logger)
        transformer.run()
    @task
    def load(start_date, end_date, logger):
        loader = Loader(start_date, end_date, logger)
        loader.run()

    start = get_start_date()
    end = calculate_end_date(start)
    logger = setup_logging()

    extract_task = extract(start, end, logger)
    transform_task = transform(start, end, logger)
    load_task = load(start, end, logger)
    update_start_date = set_start_date(end)

    extract_task >> transform_task >> load_task >> update_start_date


etl_dag()
