{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY e.event_time, e.user_id, e.product_id) AS sale_id,
    e.event_time,
    e.event_type,
    e.user_id,
    e.product_id,
    e.price,
    r.rate_EUR,
    r.rate_JPY
FROM {{ source('db_silver', 'ecommerce_data') }} as e
LEFT JOIN {{ source('db_silver', 'exchange_rates') }} as r
ON e.event_date = r.event_date
WHERE e.event_type = 'purchase'