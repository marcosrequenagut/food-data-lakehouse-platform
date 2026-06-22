import logging
from datetime import datetime, timedelta
from transformations import extract, load, transform

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Initial configuration
logger = logging.getLogger(__name__)

# DEFAULT ARGS

default_args = {
    "owner": "food_platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ============================================================
# DAG DEFINITION
# ============================================================


with DAG(
    dag_id="food_pipeline",
    description="Ingests Open Food Facts data from CSV to PostgreSQL",
    start_date=datetime(2026, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["food", "ingestion", "raw"],
) as dag:
    
    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        provide_context=True
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        provide_context=True
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True
    )

    # Order
    task_extract >> task_transform >> task_load
