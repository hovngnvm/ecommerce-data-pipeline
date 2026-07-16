{{ config(
    materialized='incremental',
    unique_key='sale_id'
) }}

WITH ranked_sales AS (
    SELECT
        event_time,
        user_id,
        product_id,
        brand,
        price,
        user_session,
        COALESCE(loyalty_tier, 'Regular') as loyalty_tier,
        COALESCE(acquisition_channel, 'Organic') as acquisition_channel,
        ROW_NUMBER() OVER (
            PARTITION BY event_time, user_id, product_id, user_session
            ORDER BY event_time
        ) as rn
    FROM {{ source('db_silver', 'ecommerce_events') }}
    WHERE event_type = 'purchase'

    {% if is_incremental() %}
    -- Only process data within a 2-day lookback window of the latest processed date to handle late arrivals/reruns
    AND event_time >= (SELECT MAX(event_time) - INTERVAL '2 days' FROM {{ this }})
    {% endif %}
)
SELECT
    md5(concat(event_time::text, '-', user_id::text, '-', product_id::text, '-', user_session, '-', rn::text)) as sale_id,
    event_time,
    user_id,
    product_id,
    brand,
    price,
    user_session,
    loyalty_tier,
    acquisition_channel
FROM ranked_sales