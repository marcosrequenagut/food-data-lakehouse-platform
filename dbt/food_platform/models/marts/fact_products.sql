-- ====================================================
-- Model: fact_products
-- Description: Central fact table with product metrics
-- Layer: marts
-- ====================================================

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

brands AS (
    SELECT * FROM {{ ref('dim_brand') }}
),

categories AS (
    SELECT * FROM {{ ref('dim_category') }}
)

SELECT
    -- Products identification
    p.code,
    p.product_name,
    p.lang,

    -- Foreign keys to dimensions
    b.brand_id,
    c.category_id,

    -- Nutrition scores
    p.nutriscore_grade,
    p.nutriscore_score,
    p.nova_group,
    p.ecoscore_grade,
    p.ecoscore_score,

    -- Nutrition values per 100g
    p.energy_kcal_100g,
    p.fat_100g,
    p.saturated_fat_100g,
    p.sugars_100g,
    p.salt_100g,
    p.proteins_100g,
    p.fiber_100g,

    -- Nutrient levels
    p.fat_level,
    p.salt_level,
    p.saturated_fat_level,
    p.sugars_level,

    -- Ingredients
    p.ingredients_n,
    p.additives_n,

    -- Dates
    p.created_at,
    p.last_modified_at,

    -- Quality
    p.completeness,

    -- Pipeline metadata
    p.ingested_at,
    p.batch_id

FROM products p
LEFT JOIN brands b
    ON {{ extract_first_tag('p.brands_tags')}} = b.brand_name
LEFT JOIN categories c
    ON p.pnns_groups_1 = c.pnns_groups_1
    AND p.pnns_groups_2 = c.pnns_groups_2