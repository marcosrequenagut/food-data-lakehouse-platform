-- =========================================================================
-- RAW LAYER — PRODUCTS OF THE CSV - WE NEED TO CREATE THE TABLE IN POSTGRES
-- =========================================================================
CREATE TABLE IF NOT EXISTS raw.products (
    id                        BIGSERIAL PRIMARY KEY,

    -- Identification
    code                      VARCHAR(50) UNIQUE,
    product_name              TEXT,
    generic_name              TEXT,
    quantity                  TEXT,
    product_quantity          NUMERIC,
    product_quantity_unit     TEXT,
    serving_size              TEXT,
    lang                      VARCHAR(10),
    url                       TEXT,

    -- Brand
    brands                    TEXT,
    brands_tags               TEXT,
    owner                     TEXT,
    manufacturing_places      TEXT,
    manufacturing_places_tags TEXT,

    -- Category
    categories                TEXT,
    categories_tags           TEXT,
    categories_hierarchy      TEXT,
    pnns_groups_1             TEXT,
    pnns_groups_2             TEXT,
    food_groups               TEXT,
    food_groups_tags          TEXT,

    -- Country
    countries                 TEXT,
    countries_tags            TEXT,
    countries_hierarchy       TEXT,
    origins                   TEXT,
    origins_tags              TEXT,
    purchase_places           TEXT,

    -- Nutrition
    nutriments                TEXT,
    nutrition_data_per        TEXT,
    nutriscore_grade          TEXT,
    nutriscore_score          NUMERIC,
    nova_group                NUMERIC,
    ecoscore_grade            TEXT,
    ecoscore_score            NUMERIC,
    nutrient_levels           TEXT,

    -- Ingredients
    ingredients_text          TEXT,
    ingredients_n             NUMERIC,
    allergens_tags            TEXT,
    traces_tags               TEXT,
    additives_n               NUMERIC,
    additives_tags            TEXT,
    ingredients_analysis_tags TEXT,
    labels                    TEXT,
    labels_tags               TEXT,

    -- Packaging
    packaging                 TEXT,
    packaging_tags            TEXT,
    packaging_materials_tags  TEXT,
    packaging_recycling_tags  TEXT,

    -- Dates
    created_t                 BIGINT,
    last_modified_t           BIGINT,
    last_updated_t            BIGINT,
    completeness              NUMERIC,

    -- Images
    image_url                 TEXT,
    image_front_url           TEXT,
    image_front_small_url     TEXT,

    -- Pipeline metadata
    ingested_at               TIMESTAMP DEFAULT NOW(),
    batch_id                  VARCHAR(50),
    source                    VARCHAR(50) DEFAULT 'csv'
);

CREATE INDEX IF NOT EXISTS idx_raw_products_code
    ON raw.products(code);
CREATE INDEX IF NOT EXISTS idx_raw_products_ingested
    ON raw.products(ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_products_batch
    ON raw.products(batch_id);