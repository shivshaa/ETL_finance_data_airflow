from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from yfinance_etl import run_yfinance_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'yfinance_dag',
    default_args=default_args,
    description='ETL pipeline to fetch data from yfinance api and then schedule jobs using airflow',
    schedule_interval='@daily',  # Run daily, adjust as needed
    catchup=False  # Don't backfill historical runs
)

run_etl = PythonOperator(
    task_id='complete_yfinance_etl',
    python_callable=run_yfinance_etl,
    dag=dag
)

run_etl