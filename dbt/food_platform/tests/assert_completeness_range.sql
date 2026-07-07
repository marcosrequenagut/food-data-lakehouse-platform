SELECT *
FROM {{ ref('stg_products') }}
WHERE completeness < 0 OR completeness > 1