{{ config(materialized='view') }}

SELECT
    TRY_CAST(user_id AS INTEGER) as user_id,
    event_type,
    TRY_CAST(product_id AS INTEGER) as product_id,
    COALESCE(category, 'unknown') as category,
    COALESCE(sub_category, 'unknown') as sub_category,
    COALESCE(brand, 'unknown') as brand,
    TRY_CAST(price AS DOUBLE) as price,
    user_session,
    TRY_CAST(event_time AS TIMESTAMP) as event_time,
    COALESCE(loyalty_tier, 'Member') as loyalty_tier,
    COALESCE(acquisition_channel, 'Organic') as acquisition_channel
FROM {{ source('db_silver', 'ecommerce_events') }}
WHERE event_time IS NOT NULL AND user_id IS NOT NULL
