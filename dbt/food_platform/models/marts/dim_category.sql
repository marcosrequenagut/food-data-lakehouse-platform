-- ===============================
-- Model: dim_category
-- Description: Category dimension
-- Layer: marts
-- ===============================

WITH categories AS (
    SELECT DISTINCT
        pnns_groups_1,
        pnns_groups_2
    FROM {{ ref('stg_products') }}
    WHERE pnns_groups_1 IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY pnns_groups_1, pnns_groups_2) AS category_id,
    pnns_groups_1,
    pnns_groups_2
FROM categories