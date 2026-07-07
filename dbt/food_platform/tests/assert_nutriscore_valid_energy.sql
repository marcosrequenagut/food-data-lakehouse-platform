SELECT *
FROM {{ ref('fact_products') }}
WHERE energy_kcal_100g < 0 OR energy_kcal_100g > 1000