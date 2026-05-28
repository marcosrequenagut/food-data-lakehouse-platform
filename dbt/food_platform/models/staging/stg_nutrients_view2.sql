-- ======================================================
-- Model: stg_products
-- Description: Cleans and standardizes raw.products data
-- Layer: staging
-- ======================================================

WITH source AS (
    SELECT * FROM raw.products
),

nutriments AS (
    SELECT
        -- New columns
        energy_kcal_100g,
        fat_100g,
        saturated_fat_100g,
        sugars_100g,
        salt_100g,
        proteins_100g,
        fiber_100g,
        nutriments,

        -- Pipeline metadata
        batch_id,
        ingested_at

    FROM source
    WHERE code IS NOT NULL
        AND product_name IS NOT NULL
)

SELECT * FROM nutriments