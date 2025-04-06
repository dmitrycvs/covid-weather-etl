from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="historical_data",
    default_args=default_args,
    start_date=datetime(2021, 4, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["historical", "weather", "covid", "etl"]
) as dag:
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extraction,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transformation,
    )

    extract_task >> transform_task