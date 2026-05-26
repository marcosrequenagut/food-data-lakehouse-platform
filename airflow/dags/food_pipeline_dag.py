import logging
import uuid
import unicodedata
from unidecode import unidecode
import numpy as np
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import os

# Initial configuration
logger = logging.getLogger(__name__)

# Execute the DAG inside the Airflow container in Docker, not in my local machine
CSV_PATH = "/opt/airflow/data/raw/openfoodfacts_sample.csv"

# Columns needed
COLUMNS = [
    "code", "product_name", "generic_name", "quantity",
    "product_quantity", "product_quantity_unit", "serving_size",
    "lang", "url", "brands", "brands_tags", "owner",
    "manufacturing_places_tags", "categories", "categories_tags",
    "categories_hierarchy", "pnns_groups_1", "pnns_groups_2",
    "food_groups", "food_groups_tags", "countries", "countries_tags",
    "countries_hierarchy", "origins", "origins_tags", "purchase_places",
    "nutriments", "nutrition_data_per", "nutriscore_grade", "nutriscore_score",
    "nova_group", "ecoscore_grade", "ecoscore_score", "nutrient_levels",
    "ingredients_text", "ingredients_n", "allergens_tags", "traces_tags",
    "additives_n", "additives_tags", "ingredients_analysis_tags",
    "labels", "labels_tags", "packaging", "packaging_tags",
    "packaging_materials_tags", "packaging_recycling_tags",
    "created_t", "last_modified_t", "last_updated_t", "completeness",
    "image_url", "image_front_url", "image_front_small_url"
]

TEXT_COLUMNS = [
    "product_name", "generic_name", "quantity", "product_quantity_unit",
    "serving_size", "lang", "brands", "owner", "manufacturing_places",
    "categories", "pnns_groups_1", "pnns_groups_2", "food_groups",
    "countries", "origins", "purchase_places", "nutrition_data_per",
    "nutriscore_grade", "ecoscore_grade", "ingredients_text",
    "labels", "packaging"
]

LIST_COLUMNS = [
    "brands_tags", "manufacturing_places_tags", "categories_tags",
    "categories_hierarchy", "food_groups_tags", "countries_tags",
    "countries_hierarchy", "origins_tags", "allergens_tags", "traces_tags",
    "additives_tags", "ingredients_analysis_tags", "labels_tags",
    "packaging_tags", "packaging_materials_tags", "packaging_recycling_tags"
]

JSON_COLUMNS = [
    "nutriments", "nutrient_levels"
]

URL_COLUMNS = [
    "url", "image_url", "image_front_url", "image_front_small_url"
]

# DEFAULT ARGS

default_args = {
    "owner": "food_platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def load(**context):  
    hook = PostgresHook(PostgresHook_conn_id="food_postgres")
    conn = hook.get_conn()

def clean_text(x):
    """
    Transforms a string into lowecase and remove special characters
    """
    if pd.isna(x):
        return x
    return unidecode(str(x).lower())

def extract(**context):
    """
    Reads CSV file and validates is has data.
    Pushes batch_id and row count to Xcom
    """

    logger.info(f"Reading CSV from : {CSV_PATH}")

    # Validate if CSV exists
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV nod found at: {CSV_PATH}")
    
    # Read CSV
    df = pd.read_csv(CSV_PATH)
    logger.info(f"CSV loaded: {df.shape[0]} rows, {df.shape[1]} columns")

    # Validate not empty
    if len(df) == 0:
        raise ValueError("CSV is empty")
    
    # Keep only needed columns
    df = df[COLUMNS]
    logger.info(f"Columns filtered: {len(COLUMNS)} columns kept")

    # Generate unique batch_id for this run
    batch_id = f"batch_id{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    logger.info(f"Batch ID: {batch_id}")

    # Save extracted data to temp file
    temp_path = "/opt/airflow/data/raw/temp_extract.csv"
    df.to_csv(temp_path, index=False)

    # Push metadata to XCom - NOT the dataframe, just small values
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="temp_path", value=temp_path)
    context["ti"].xcom_push(key="total_rows", value=len(df))

def transform(**context):
    """
    Cleans and transforms raw data.
    - Drops rows without code or product name.
    - Normalice the completeness column with values between 0 and 1
    - Normalizes text columns (lowercase + use strip() + remove special characters)
    - Replaces 'xx' with 'unknown' in the lang column.
    - Converts empty tags [] to NaN values in the manufacturing_places_tags column.
    - Keep column_tags and remove columns.
    """

    # Pull metadata from previous task
    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract")
    temp_path = context["ti"].xcom_pull(key="temp_path", task_ids="extract")

    logger.info(f"\nStarting transform for batch: {batch_id}")

    df = pd.read_csv(temp_path)
    initial_rows = len(df)
    logger.info(f"Rows before cleaning: {initial_rows}")

    # Drops rows without code or product name.
    df = df.dropna(subset=["code", "product_name"])
    rows_1 = len(df)
    logger.info(f"NaN values remove from code and product name columns. Rows: {rows_1}")

    # Normalice the completeness column with values between 0 and 1
    df["completeness"] = df["completeness"].clip(0, 1)
    logger.info(f"Normalice the completeness column with values between 0 and 1.")

    # Normalizes text columns (lowercase  + remove special characters)
    for column in df.columns:
        if column in TEXT_COLUMNS:
            df[column] = df[column].apply(clean_text)
    logger.info(f"Texts columns normalized")

    # Replaces 'xx' with 'unknown' in the lang column.
    df["lang"] = df["lang"].replace("xx", "unknown")
    logger.info(f"Column lang transformed")

    # Converts empty tags [] of the column manufacturing_places_tags to NaN values.
    df["manufacturing_places_tags"] = df["manufacturing_places_tags"].apply(lambda x: np.nan if x == [] or '[]' else x)
    logger.info(f"Column manufacturing_places_tags transformed")

    # Keep column_tags and remove columns.
    columns_tags = [col for col in df.columns if col.endswith('_tags')]
    normalized_columns_tags = [col.replace("_tags", "") for col in columns_tags]
    logger.info(f" normalized_columns_tags : {normalized_columns_tags}")
    columns_non_tags = [col for col in df.columns if col not in columns_tags]

    columns_to_remove = []
    for column_non_tag in columns_non_tags:
        if column_non_tag in normalized_columns_tags:
            columns_to_remove.append(column_non_tag)

    df = df.drop(columns=columns_to_remove)
    logger.info(f"{len(columns_to_remove)} Columns removed: {columns_to_remove}")

    # Add pipeline metadata
    df["batch_id"] = batch_id
    df["ingested_at"] = datetime.now()

    final_rows = len(df)
    logger.info(f"Transformation complete: {initial_rows} -> {final_rows} rows")

    # Save clean data
    clean_path = "/opt/airflow/data/raw/temp_clean.csv"
    df.to_csv(clean_path)

    # Push to Xcom
    context["ti"].xcom_push(key="clean_path", value=clean_path)
    context["ti"].xcom_push(key="clean_rows", value=final_rows)

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
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    # Order
    task_extract >> task_transform