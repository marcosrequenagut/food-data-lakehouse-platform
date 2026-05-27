-- ======================================================
-- Model: stg_products
-- Description: Cleans and standardizes raw.products data
-- Layer: staging
-- ======================================================

WITH source AS (
    SELECT * FROM raw.products
),

cleaned AS (
    SELECT
        -- Identification
        code,
        product_name,
        generic_name,
        quantity,
        product_quantity,
        product_quantity_unit,
        serving_size,
        lang,
        url,

        -- Brand
        brands_tags,
        owner,
        manufacturing_places_tags,

        -- Category
        categories_tags,
        categories_hierarchy,
        pnns_groups_1,
        pnns_groups_2,
        food_groups_tags,

        -- Country
        countries_tags,
        countries_hierarchy,
        origins_tags,
        purchase_places,

        -- Nutrition
        nutriments,
        nutrition_data_per,
        nutriscore_grade,
        nutriscore_score::NUMERIC       AS nutriscore_score,
        nova_group::NUMERIC             AS nova_group,
        ecoscore_grade,
        ecoscore_score::NUMERIC         AS ecoscore_score,
        nutrient_levels,

        -- Ingredients
        ingredients_text,
        ingredients_n::NUMERIC          AS ingredients_n,
        allergens_tags,
        traces_tags,
        additives_n::NUMERIC            AS additives_n,
        additives_tags,
        ingredients_analysis_tags,

        -- Labels & Packaging
        labels_tags,
        packaging_tags,
        packaging_materials_tags,
        packaging_recycling_tags,

        -- Dates — convert Unix timestamps to datetime
        TO_TIMESTAMP(created_t)         AS created_at,
        TO_TIMESTAMP(last_modified_t)   AS last_modified_at,
        TO_TIMESTAMP(last_updated_t)    AS last_updated_at,

        -- Quality
        completeness,

        -- Images
        image_url,
        image_front_url,
        image_front_small_url,

        -- New columns
        fat_level,
        salt_level,
        saturated_fat_level,
        sugars_level

        -- Pipeline metadata
        batch_id,
        ingested_at

    FROM source
    WHERE code IS NOT NULL
        AND product_name IS NOT NULL
)

SELECT * FROM cleaned