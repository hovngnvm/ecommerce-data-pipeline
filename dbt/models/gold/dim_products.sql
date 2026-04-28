{{ config(materialized='table') }}

SELECT DISTINCT ON (product_id)
    product_id,
    category,
    sub_category,
    brand
FROM {{ ref('stg_events') }}
WHERE product_id IS NOT NULL
ORDER BY
    product_id,
    (category != 'unknown' AND sub_category != 'unknown' AND brand != 'unknown') DESC

