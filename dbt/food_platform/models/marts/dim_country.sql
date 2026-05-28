SELECT
    ROW_NUMBER() OVER (ORDER BY country_code) AS country_id,
    country_code,
    country_name
FROM(
    SELECT DISTINCT
        country_code,
        country_name
    FROM {{ ref('bridge_product_country') }}
) AS countries