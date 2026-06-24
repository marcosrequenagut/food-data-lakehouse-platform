-- Model: mart_brand_quality
-- Description: Brand ranking by nutriscore and ecoscore
-- Layer: marts

WITH nutrients AS (
    SELECT * FROM {{ ref('stg_nutrients') }}
),

brands AS (
    SELECT * FROM {{ ref('bridge_product_brand') }}
),

joined AS (
    SELECT
        UPPER(b.brand_name),
        COUNT(b.code)                           AS total_products,
        ROUND(AVG(n.energy_kcal_100g), 2)       AS avg_energy_kcal,
        ROUND(AVG(n.fat_100g), 2)               AS avg_fat,
        ROUND(AVG(n.saturated_fat_100g), 2)     AS avg_saturated_fat,
        ROUND(AVG(n.sugars_100g), 2)            AS avg_sugars,
        ROUND(AVG(n.salt_100g), 2)              AS avg_salt,
        ROUND(AVG(n.proteins_100g), 2)          AS avg_proteins,
        ROUND(AVG(n.fiber_100g), 2)             AS avg_fiber
    FROM brands b
    INNER JOIN nutrients n ON b.code = n.code
    GROUP BY UPPER(b.brand_name)
    HAVING COUNT(b.code) > 2 -- only brands with at leats 2 products
    ORDER BY avg_energy_kcal
)

SELECT * FROM joined