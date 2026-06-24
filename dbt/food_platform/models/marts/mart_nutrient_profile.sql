with products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

nutrients AS (
    SELECT * FROM {{ ref('stg_nutrients') }}
),

joined AS (
    SELECT
        p.pnns_groups_1,
        COUNT(p.code)                           AS total_products,
        ROUND(AVG(n.energy_kcal_100g), 2)       AS avg_energy_kcal,
        ROUND(AVG(n.fat_100g), 2)               AS avg_fat,
        ROUND(AVG(n.saturated_fat_100g), 2)     AS avg_saturated_fat,
        ROUND(AVG(n.sugars_100g), 2)            AS avg_sugars,
        ROUND(AVG(n.salt_100g), 2)              AS avg_salt,
        ROUND(AVG(n.proteins_100g), 2)          AS avg_proteins,
        ROUND(AVG(n.fiber_100g), 2)             AS avg_fiber
    FROM products p
    INNER JOIN nutrients n ON p.code = n.code
    WHERE p.pnns_groups_1 IS NOT NULL
    GROUP BY p.pnns_groups_1
)

SELECT * FROM JOINED