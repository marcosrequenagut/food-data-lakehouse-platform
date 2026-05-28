
WITH source AS (
    SELECT * FROM raw.products
),

nutrients AS (
    SELECT
        -- New columns
        fat_level,
        salt_level,
        saturated_fat_level,
        sugars_level,
        nutrient_levels,

        -- Pipeline metadata
        batch_id,
        ingested_at

    FROM source
    WHERE code IS NOT NULL
        AND product_name IS NOT NULL
)

SELECT * FROM nutrients