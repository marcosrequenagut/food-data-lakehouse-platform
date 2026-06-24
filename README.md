# 🍎 Food Data Lakehouse Platform

A end-to-end data engineering project built on top of the **Open Food Facts API**, simulating a real company data platform.

---

## 📌 Overview

This project ingests, cleans, models and visualizes food product data from the Open Food Facts API. It covers the full data engineering lifecycle, from raw ingestion to an interactive analytics dashboard.

---

## 🏗️ Architecture

```
Open Food Facts API
        ↓
[Python Ingestion Script]
        ↓
RAW LAYER — PostgreSQL (raw.products)
        ↓
STAGING LAYER — dbt (stg_products, stg_nutrients)
        ↓
MARTS LAYER — dbt (star schema + aggregation marts)
        ↓
AUDIT LAYER — dbt Hooks (audit.model_runs)
        ↓
DASHBOARD — Streamlit
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt Core + dbt-utils |
| Database | PostgreSQL 15 |
| Language | Python 3.9 |
| Dashboard | Streamlit |
| Infrastructure | Docker + Docker Compose |
| CI/CD | GitHub Actions + pytest |
| Version Control | Git + GitHub |

---

## 📁 Project Structure

```
food-data-lakehouse-platform/
│
├── airflow/
│   ├── dags/
│   │   ├── food_pipeline_dag.py       # DAG definition
│   │   └── transformations.py         # Extract, Transform, Load functions
│   ├── logs/
│   └── plugins/
│
├── dbt/
│   └── food_platform/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_products.sql       # Staging view — datos generales
│       │   │   ├── stg_nutrients.sql      # Staging view — columnas nutricionales
│       │   │   └── sources.yml            # Source definitions + freshness checks
│       │   └── marts/
│       │       ├── dim_brand.sql
│       │       ├── dim_category.sql
│       │       ├── dim_country.sql
│       │       ├── bridge_product_country.sql
│       │       ├── bridge_product_brand.sql
│       │       ├── fact_products.sql
│       │       ├── mart_nutrient_profile.sql
│       │       └── mart_brand_quality.sql
│       ├── macros/
│       │   ├── generate_schema_name.sql
│       │   └── extract_first_tag.sql
│       ├── snapshots/
│       │   └── products_snapshot.sql
│       ├── tests/
│       │   └── assert_*.sql               # Singular tests
│       └── profiles.yml
│
├── postgres/
│   └── init/
│       ├── 01_init_schemas.sql        # Create schemas
│       └── 02_init_tables.sql         # Create tables
│
├── streamlit/
│   └── app.py                         # Interactive dashboard
│
├── notebooks/
│   ├── 01_eda_openfoodfacts.ipynb     # Exploratory data analysis
│   └── 02_cleaning_transformations.ipynb
│
├── tests/
│   └── test_transform.py              # Unit tests
│
├── .github/
│   └── workflows/
│       └── ci.yml                     # CI pipeline
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 🔄 Pipeline

The Airflow DAG `food_pipeline` runs daily and executes 6 tasks:

```
[extract] → [transform] → [load] → [dbt_run] → [dbt_snapshot] → [dbt_test]
```

### Extract
- Reads data from Open Food Facts API (10 pages × 100 products)
- Validates file exists and is not empty
- Generates a unique `batch_id` for traceability
- Passes metadata to next task via XCom

### Transform
Applies the following cleaning rules:

| Rule | Description |
|------|-------------|
| DQ-001 | Replace `xx` with `unknown` in `lang` column |
| DQ-002 | Clip `completeness` to 0-1 range |
| DQ-003 | Drop rows without `code` or `product_name` |
| DQ-004 | Drop duplicate products by `code` |
| TRANSFORM-001 | Normalize text columns (lowercase + remove accents) |
| TRANSFORM-002 | Convert empty tags `[]` to `None` |
| TRANSFORM-003 | Parse `nutrient_levels` JSON into separate columns |
| TRANSFORM-004 | Parse `nutriments` JSON into 7 nutritional columns |

### Load
- Connects to PostgreSQL using Airflow Connections
- Inserts clean data into `raw.products`
- Uses `ON CONFLICT (code) DO NOTHING` to avoid duplicates
- Logs loaded and failed rows

### dbt Run
- Materializes all staging and mart models
- `on-run-start` automatically creates `audit.model_runs` if it does not exist
- `post-hook` logs each materialized model into the audit table

### dbt Snapshot
- Executes `products_snapshot` to capture product changes (SCD Type 2)
- Tracks: nutriscore_grade, ecoscore_grade, nova_group, nutrient levels, owner

### dbt Test
- Runs all generic and singular tests defined in `schema.yml` and `tests/`

---

## 🗄️ Data Model

### Staging Layer

| Model | Schema | Type | Description |
|-------|--------|------|-------------|
| `stg_products` | staging | View | Cleaned and standardized general product data |
| `stg_nutrients` | staging | View | Cleaned nutritional columns (100g values + levels) |

### Marts Layer (Star Schema + Aggregations)

```
                    dim_brand
                        ↑
bridge_product_brand ───┤
                        │
dim_category ←── fact_products ──→ bridge_product_country ──→ dim_country
                        │
                   stg_nutrients
                        ↓
              mart_nutrient_profile
              mart_brand_quality
```

| Table | Schema | Type | Description |
|-------|--------|------|-------------|
| `dim_brand` | marts | Table | Brand dimension (without code, pure dimension) |
| `dim_category` | marts | Table | Category dimension (pnns groups) |
| `dim_country` | marts | Table | Country dimension |
| `bridge_product_country` | marts | Table | Product-country many-to-many |
| `bridge_product_brand` | marts | Table | Product-brand many-to-many |
| `fact_products` | marts | Table | Central fact table with metrics |
| `mart_nutrient_profile` | marts | Table | Nutritional average profile per category Perfil pnns_groups_1 |
| `mart_brand_quality` | marts | Table | Ranking of brands by nutritional values |

### Audit Layer

| Table | Schema | Type | Description |
|-------|--------|------|-------------|
| `model_runs` | audit | Table | Automatic log of each materialized model |

Automatically created with on-run-start on every dbt run. The post-hook inserts a row per marts model with its name and timestamp.

---

## 🔧 dbt Features Implemented

| Feature | Description |
|---------|-------------|
| Sources | `sources.yml` con freshness checks (warn: 24h, error: 48h) |
| Macros | `extract_first_tag(column_name)` — extracts and cleans the first columns tag `_tags` |
| Snapshots | `products_snapshot` — SCD Type 2 about products changes |
| Generic Tests | `not_null`, `unique`, `accepted_values`, `relationships` |
| Singular Tests | Personalized SQL Tests with business logic|
| Hooks | `on-run-start` + `post-hook` for automatic auditories |
| Schema separation | Macro `generate_schema_name` to avoid dbt prefixes |

---

## 📊 Dashboard

Interactive Streamlit dashboard with:

- **Global filters** : country, category, nutriscore grade, nova group, calorie range
- **Dynamic KPIs** : total products, average nutriscore, average calories
- **Interactive table** : sortable, searchable product explorer
- **Charts** : nutriscore distribution, top brands, nova group distribution

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Python 3.9+
- Git

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/food-data-lakehouse-platform.git
cd food-data-lakehouse-platform
```

### 2. Start the stack
```bash
docker-compose up --build -d
```

### 3. Access Airflow UI
```
URL:      http://localhost:8080
User:     admin
Password: admin123
```

### 4. Initialize the database
```bash
docker exec -it postgres_food psql -U food_user -d food_platform -f /docker-entrypoint-initdb.d/01_init_schemas.sql
docker exec -it postgres_food psql -U food_user -d food_platform -f /docker-entrypoint-initdb.d/02_init_tables.sql
```

### 5. Add Airflow connection
In Airflow UI → Admin → Connections → Add:
```
Connection Id:   food_postgres
Connection Type: Postgres
Host:            postgres-food
Database:        food_platform
Login:           food_user
Password:        food_password123
Port:            5432
```

### 6. Trigger the DAG
In Airflow UI → DAGs → `food_pipeline` → Trigger

### 7. Run dbt models
```bash
cd dbt/food_platform
dbt run --profiles-dir .
```

### 8. Launch dashboard
```bash
cd streamlit
streamlit run app.py
```

---

## 🧪 Testing

```bash
pytest tests/ -v
```

CI/CD pipeline runs automatically on every push to `develop` and `main` via GitHub Actions.

---

## 📡 Data Source

[Open Food Facts](https://world.openfoodfacts.org/) — open database of food products worldwide.

- 3M+ products
- Updated daily
- Complex and dirty JSON data (ideal for data engineering practice)

---

## 📈 Data Quality Issues Found

| ID | Column | Issue | Action |
|----|--------|-------|--------|
| DQ-001 | `lang` | `xx` used for unknown language | Replace with `unknown` |
| DQ-002 | `completeness` | Values > 1 found | Clip to 0-1 range |
| DQ-003 | `code`, `product_name` | Null values in key columns | Drop rows |
| DQ-004 | `code` | Duplicate products | Drop duplicates |
| DQ-005 | `*_tags` | Empty lists `[]` instead of null | Convert to `None` |
| DQ-006 | `nutriments` | Nested JSON not queryable | Parse into separate columns |
| DQ-007 | `nutrient_levels` | Nested JSON not queryable | Parse into 4 level columns |

---

## 👤 Author
Juan Marcos Requena Gutiérrez |
Built as a mid-level data engineering portfolio project.
