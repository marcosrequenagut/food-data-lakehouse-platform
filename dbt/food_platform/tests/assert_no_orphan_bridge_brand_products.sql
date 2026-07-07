SELECT *
FROM {{ ref('bridge_product_brand') }} t1
LEFT JOIN {{ ref('stg_products') }} t2 ON t1.code = t2.code
WHERE t2.code IS NULL