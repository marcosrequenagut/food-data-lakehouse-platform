-- Model: dim_brand
-- Description: Brand dimension
-- Layer: marts

with brands AS (
    SELECT DISTINCT
        {{ extract_first_tag('brands_tags') }} AS brand_name,
        owner
    FROM {{ ref('stg_products') }}
    WHERE brands_tags IS NOT NULL
        AND brands_tags != '[]'
)

SELECT
    ROW_NUMBER() OVER (ORDER BY brand_name) AS brand_id,
    brand_name,
    owner
FROM brands
WHERE brand_name IS NOT NULL
    AND BRAND_NAME != ''