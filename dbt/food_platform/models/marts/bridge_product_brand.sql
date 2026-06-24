-- Model: bridge_product_brand
-- Description: Many-to-many relationship between products and brands
-- Layer: marts

WITH exploded AS (
    SELECT
        code,
        TRIM(
            REPLACE(
                REPLACE(
                    UNNEST(STRING_TO_ARRAY(
                        REPLACE(REPLACE(REPLACE(brands_tags, '[', ''), ']', ''), '''', ''),
                        ','
                    )),
                '"', ''),
            ' ', '')
        ) AS brand_tag
    FROM {{ ref('stg_products') }}
    WHERE brands_tags IS NOT NULL
        AND brands_tags != '[]'
)

SELECT 
    code,
    brand_tag AS brand_name
FROM exploded
WHERE brand_tag IS NOT NULL
    AND TRIM(brand_tag) != ''