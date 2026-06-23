import uuid
from unidecode import unidecode
import numpy as np
from datetime import datetime
import pandas as pd
import os
import ast
import logging


# Initial configuration
logger = logging.getLogger(__name__)

# Execute the DAG inside the Airflow container (Docker), not in my local machine
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


def clean_text(x):
    """
    Transforms a string into lowecase and remove special characters
    """
    if pd.isna(x):
        return x
    return unidecode(str(x).lower().strip())


def parse_nutrient_levels(x):
    """
    Extracts fat, salt, saturated_fat and sugars from nutrient_levels dict.
    """
    if x is None or x == "" or (isinstance(x, float) and pd.isna(x)):
        return None, None, None, None
    try:
        # Convert string to dict first
        if isinstance(x, str):
            d = ast.literal_eval(x)
        else:
            d = x
        return (
            d.get("fat", "unknown"),
            d.get("salt", "unknown"),
            d.get("saturated-fat", "unknown"),
            d.get("sugars", "unknown")
        )
    except Exception as e:
        print(f"Error: {e}, value: {x}")
        return None, None, None, None


def parse_nutrients(x):
    """
    Extracts energy-kcal_100g", "fat_100g", "saturated-fat_100g",
    "sugars_100g", "salt_100g", "proteins_100g",
    "fiber_100g from nutrient dict.
    """
    if x is None or x == "" or (isinstance(x, float) and pd.isna(x)):
        return (np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)
    try:
        # Convert string to dict first
        if isinstance(x, str):
            d = ast.literal_eval(x)
        else:
            d = x
        return (
            d.get("energy-kcal_100g", np.nan),
            d.get("fat_100g", np.nan),
            d.get("saturated-fat_100g", np.nan),
            d.get("sugars_100g", np.nan),
            d.get("salt_100g", np.nan),
            d.get("proteins_100g", np.nan),
            d.get("fiber_100g", np.nan),
        )
    except Exception as e:
        print(f"Error: {e}, value: {x}")
        return (np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)


def fetch_page(session, url, params, headers, max_retries=3):
    """
    Try to load a page from the API with retries.
    max_retries=3 -> initial try + 2 retries
    """
    import time
    import requests

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=30
            )

            if response.status_code == 200:
                return response.json()

            logger.warning(
                f"Page {params.get('page')} failed (status {response.status_code}), "
                f"attempt {attempt}/{max_retries}. Retrying..."
            )

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Request exception on page {params.get('page')}: {e}, "
                f"attempt {attempt}/{max_retries}. Retrying..."
            )

        # If it is not the last retry -> wait 1s
        if attempt < max_retries:
            time.sleep(1)

    logger.error(f"Page {params.get('page')} failed after {max_retries} attempts")
    return None


def extract(**context):
    """
    Fetches data from Open Food Facts API and saves it as CSV.
    If it fails, it retries 2 times with a 5 seconds delay.
    Pushes batch_id and row count to Xcom
    """
    import requests
    import time

    # Bring 100 products of the page 1
    session = requests.Session()
    API_URL = "https://world.openfoodfacts.org/api/v2/search"
    HEADERS = {
            "User-Agent": "Mozilla/5.0 (Data Engineering Project - student)"
        }
    TOTAL_PAGES = 10

    logger.info("Starting extraction from Open Food Facts API...")
    all_products = []

    for page in range(1, TOTAL_PAGES + 1):
        logger.info(f"Fetching page {page}/{TOTAL_PAGES}...")
        params = {
            "page_size": 100,
            "page": page
        }

        data = fetch_page(session, API_URL, params, HEADERS)
        if data:
            products = data.get("products", [])
            all_products.extend(products)
            logger.info(f"  → {len(products)} products fetched. Total: {len(all_products)}")
        else:
            logger.error(f"  → Failed to fetch page {page}, skipping...")

        time.sleep(1)  # Respect the limit rate of the API

    logger.info(f"\nTotal products fetched: {len(all_products)}")

    # Save to a CSV
    df = pd.DataFrame(all_products)
    project_root = os.path.dirname(os.path.abspath("__file__"))
    output_path = os.path.join(project_root, "data", "raw", "openfoodfacts_sample.csv")

    # Create the directory if it doesn't exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to: {output_path}")
    logger.info("Saved to CSV!")

    logger.info(f"Reading CSV from : {CSV_PATH}")

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
    - Extracts fat, salt, saturated_fat and sugars from nutrient_levels dict.
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
    logger.info("Normalice the completeness column with values between 0 and 1.")

    # Normalizes text columns (lowercase  + remove special characters)
    for column in df.columns:
        if column in TEXT_COLUMNS:
            df[column] = df[column].apply(clean_text)
    logger.info("Texts columns normalized")

    # Replaces 'xx' with 'unknown' in the lang column.
    df["lang"] = df["lang"].replace("xx", "unknown")
    logger.info("Column lang transformed")

    # Converts empty tags [] of the column manufacturing_places_tags to NaN values.
    df["manufacturing_places_tags"] = (
        df["manufacturing_places_tags"]
        .apply(lambda x: np.nan if x == [] or '[]' else x)
    )
    logger.info("Column manufacturing_places_tags transformed")

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

    # Extracts fat, salt, saturated_fat and sugars from nutrient_levels dict.
    cols = ["fat_level", "salt_level", "saturated_fat_level", "sugars_level"]
    df[cols] = (
        df["nutrient_levels"]
        .apply(lambda x: pd.Series(parse_nutrient_levels(x)))
    )
    logger.info("COLUMNS fat_level, salt_level, saturated_fat_level, sugars_level CREATED")

    # Extracts energy-kcal_100g", "fat_100g", "saturated-fat_100g",
    # "sugars_100g", "salt_100g", "proteins_100g", "fiber_100g from nutrient dict.
    df[["energy_kcal_100g", "fat_100g", "saturated_fat_100g",
        "sugars_100g", "salt_100g", "proteins_100g", "fiber_100g"]] = (
            df["nutriments"]
            .apply(lambda x: pd.Series(parse_nutrients(x)))
            )
    logger.info(
        "COLUMNS energy_kcal_100g, fat_100g, saturated_fat_100g,"
        "sugars_100g, salt_100g, proteins_100g, fiber_100g CREATED"
    )

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


# TASK3: LOAD
def load(**context):
    """
    Loads clean data into a PostrgreSQL DB raw.products table
    """

    from airflow.hooks.postgres_hook import PostgresHook

    batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="extract")
    clean_path = context["ti"].xcom_pull(key="clean_path", task_ids="transform")
    clean_rows = context["ti"].xcom_pull(key="clean_rows", task_ids="transform")

    logger.info(f"Loading {clean_rows} rows to PostgreSQL. Batch: {batch_id}")

    df = pd.read_csv(clean_path)

    logger.info(f"COLUMNAS DEL DF: {df.columns}")

    # Connect using Aiflow Connection
    hook = PostgresHook(postgres_conn_id="food_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()

    loaded = 0
    failed = 0

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO raw.products (
                    code, product_name, generic_name, quantity,
                    product_quantity, product_quantity_unit, serving_size,
                    lang, url, brands_tags, owner,
                    manufacturing_places_tags, categories_tags,
                    categories_hierarchy, pnns_groups_1, pnns_groups_2,
                    food_groups_tags, countries_tags,
                    countries_hierarchy, origins_tags, purchase_places,
                    nutriments, nutrition_data_per, nutriscore_grade,
                    nutriscore_score, nova_group, ecoscore_grade,
                    ecoscore_score, nutrient_levels,
                    ingredients_text, ingredients_n, allergens_tags, traces_tags,
                    additives_n, additives_tags, ingredients_analysis_tags,
                    labels_tags, packaging_tags,
                    packaging_materials_tags, packaging_recycling_tags,
                    created_t, last_modified_t, last_updated_t, completeness,
                    image_url, image_front_url, image_front_small_url,
                    batch_id, ingested_at, fat_level, salt_level,
                    saturated_fat_level, sugars_level, energy_kcal_100g,
                    fat_100g, saturated_fat_100g, sugars_100g, salt_100g,
                    proteins_100g, fiber_100g
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (code) DO NOTHING
            """, tuple(
                None if pd.isna(v) else v for v in [
                    row.get("code"), row.get("product_name"), row.get("generic_name"),
                    row.get("quantity"), row.get("product_quantity"),
                    row.get("product_quantity_unit"),
                    row.get("serving_size"), row.get("lang"), row.get("url"),
                    row.get("brands_tags"), row.get("owner"),
                    row.get("manufacturing_places_tags"),
                    row.get("categories_tags"), row.get("categories_hierarchy"),
                    row.get("pnns_groups_1"), row.get("pnns_groups_2"),
                    row.get("food_groups_tags"),
                    row.get("countries_tags"),
                    row.get("countries_hierarchy"),
                    row.get("origins_tags"), row.get("purchase_places"),
                    row.get("nutriments"), row.get("nutrition_data_per"),
                    row.get("nutriscore_grade"), row.get("nutriscore_score"),
                    row.get("nova_group"), row.get("ecoscore_grade"),
                    row.get("ecoscore_score"), row.get("nutrient_levels"),
                    row.get("ingredients_text"), row.get("ingredients_n"),
                    row.get("allergens_tags"), row.get("traces_tags"),
                    row.get("additives_n"), row.get("additives_tags"),
                    row.get("ingredients_analysis_tags"),
                    row.get("labels_tags"),
                    row.get("packaging_tags"), row.get("packaging_materials_tags"),
                    row.get("packaging_recycling_tags"), row.get("created_t"),
                    row.get("last_modified_t"), row.get("last_updated_t"),
                    row.get("completeness"), row.get("image_url"),
                    row.get("image_front_url"), row.get("image_front_small_url"),
                    row.get("batch_id"), row.get("ingested_at"),
                    row.get("fat_level"), row.get("salt_level"),
                    row.get("saturated_fat_level"), row.get("sugars_level"),
                    row.get("energy_kcal_100g"), row.get("fat_100g"),
                    row.get("saturated_fat_100g"), row.get("sugars_100g"),
                    row.get("salt_100g"), row.get("proteins_100g"),
                    row.get("fiber_100g")
                ]
            ))
            loaded += 1
        except Exception as e:
            logger.error(f"Error inserting rwo {row.get('code')}: {e}")
            failed += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Load complete - loaded: {loaded}, failed: {failed}")
