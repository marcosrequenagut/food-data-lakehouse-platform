import logging
from datetime import datetime, timedelta
from transformations import extract, load, transform

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Initial configuration
logger = logging.getLogger(__name__)

# DEFAULT ARGS

default_args = {
    "owner": "food_platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt/food_platform"

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

    task_dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir ."
    )

    # Order
    (
        task_extract
        >> task_transform
        >> task_load
        >> task_dbt_build
    )
