-- =====================================================================
-- Model: bridge_product_country
-- Description: Many-to-many relationship between products and countries
-- Layer: marts
-- =====================================================================

WITH exploded AS (
    SELECT
        code,
        TRIM(
            REPLACE(
                REPLACE(
                    UNNEST(STRING_TO_ARRAY(
                        REPLACE(REPLACE(REPLACE(countries_tags, '[', ''), ']', ''), '''', ''),
                        ','
                    )),
                '"', ''),
            ' ', '')
        ) AS country_code
    FROM {{ ref('stg_products') }}
    WHERE countries_tags IS NOT NULL
        AND countries_tags != '[]'
)

SELECT
    code,
    country_code,
    REPLACE(country_code, 'en:', '') AS country_name
FROM exploded
WHERE country_code IS NOT NULL
    AND country_code != ''