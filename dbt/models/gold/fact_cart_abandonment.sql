{{ config(
    materialized='incremental',
    unique_key='abandonment_id'
) }}

WITH cart_events AS (
    SELECT
        user_id,
        user_session,
        product_id,
        brand,
        price,
        event_time AS cart_time,
        loyalty_tier,
        acquisition_channel,
        ROW_NUMBER() OVER (
            PARTITION BY user_session, product_id, event_time
            ORDER BY event_time
        ) as rn
    FROM {{ source('db_silver', 'ecommerce_events') }}
    WHERE event_type = 'cart'

    {% if is_incremental() %}
    -- Only process cart events within a 2-day lookback window of the latest processed date
    AND event_time >= (SELECT MAX(cart_time) - INTERVAL '2 days' FROM {{ this }})
    {% endif %}
),

purchase_events AS (
    SELECT DISTINCT
        user_session,
        product_id
    FROM {{ source('db_silver', 'ecommerce_events') }}
    WHERE event_type = 'purchase'

    {% if is_incremental() %}
    -- Filter purchase events within the same lookback window
    AND event_time >= (SELECT MAX(cart_time) - INTERVAL '2 days' FROM {{ this }})
    {% endif %}
)

SELECT
    md5(concat(c.user_session, '-', c.product_id, '-', c.cart_time::text, '-', c.rn::text)) as abandonment_id,
    c.user_id,
    c.user_session,
    c.product_id,
    c.brand,
    c.price,
    c.cart_time,
    coalesce(c.loyalty_tier, 'Regular') as loyalty_tier,
    coalesce(c.acquisition_channel, 'Organic') as acquisition_channel
FROM cart_events c
LEFT JOIN purchase_events p
  ON c.user_session = p.user_session 
  AND c.product_id = p.product_id
WHERE p.user_session IS NULL
