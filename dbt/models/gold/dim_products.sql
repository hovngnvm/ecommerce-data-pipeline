{{ config(materialized='table') }}

WITH ranked_products AS (
    SELECT DISTINCT 
        product_id,
        category,
        sub_category,
        brand,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY COALESCE(category, '') DESC, COALESCE(sub_category, '') DESC, COALESCE(brand, '') DESC
        ) as rn
    FROM {{ source('db_silver', 'ecommerce_events') }}
    WHERE product_id IS NOT NULL
)
SELECT 
    product_id,
    category,
    sub_category,
    brand
FROM ranked_products
WHERE rn = 1