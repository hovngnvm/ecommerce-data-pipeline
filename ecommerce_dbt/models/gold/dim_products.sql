{{ config(materialized='table') }}

SELECT DISTINCT 
    product_id,
    category_id,
    category,
    sub_category,
    brand
FROM {{ source('db_silver', 'ecommerce_data') }}
WHERE product_id IS NOT NULL