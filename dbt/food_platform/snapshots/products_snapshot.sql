{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='code',
        strategy='timestamp',
        updated_at='last_modified_at'
    )
}}

SELECT
code,
    product_name,
    nutriscore_grade,
    nutriscore_score,
    nova_group,
    ecoscore_grade,
    ecoscore_score,
    fat_level,
    salt_level,
    saturated_fat_level,
    sugars_level,
    owner,
    brands_tags,
    completeness,
    last_modified_at
FROM {{ ref('stg_products') }}

{% endsnapshot %}





