SELECT *
FROM {{ ref('fact_products') }}
WHERE fat_100g > 100
    OR sugars_100g > 100
    OR proteins_100g > 100
    OR salt_100g > 100