
WITH source AS (
    SELECT * FROM {{ source('raw', 'products')}}
),

nutrients AS (
    SELECT
        -- Primary key (to make joins from Marts models)
        code,
        
        -- Nutrient values per 100g
        energy_kcal_100g,
        fat_100g,
        saturated_fat_100g,
        sugars_100g,
        salt_100g,
        proteins_100g,
        fiber_100g,
        
        -- Nutrient levels (categorial: low/moderate/high)
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