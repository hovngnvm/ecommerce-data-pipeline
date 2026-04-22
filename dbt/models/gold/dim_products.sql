{{ config(materialized='table') }}

WITH ranked_products AS (
    SELECT
        product_id,
        category,
        sub_category,
        brand,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE WHEN category IS NOT NULL AND category != 'unknown' THEN 0 ELSE 1 END,
                CASE WHEN sub_category IS NOT NULL AND sub_category != 'unknown' THEN 0 ELSE 1 END,
                CASE WHEN brand IS NOT NULL AND brand != 'unknown' THEN 0 ELSE 1 END,
                COALESCE(category, '') DESC,
                COALESCE(sub_category, '') DESC,
                COALESCE(brand, '') DESC
        ) as rn
    FROM {{ ref('stg_events') }}
    WHERE product_id IS NOT NULL
)
SELECT
    product_id,
    category,
    sub_category,
    brand
FROM ranked_products
WHERE rn = 1
