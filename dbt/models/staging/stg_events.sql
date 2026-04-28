{{ config(materialized='view') }}

SELECT
    user_id,
    event_type,
    product_id,
    category,
    sub_category,
    brand,
    price,
    user_session,
    event_time,
    loyalty_tier,
    acquisition_channel
FROM {{ source('db_silver', 'ecommerce_events') }}
WHERE event_time IS NOT NULL AND user_id IS NOT NULL

