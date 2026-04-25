{{ config(
    materialized='table'
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
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'cart'
),

purchase_events AS (
    SELECT
        user_session,
        product_id,
        event_time AS purchase_time
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'purchase'
)

SELECT
    md5(concat(c.user_session, '-', c.product_id::text, '-', c.cart_time::text)) as abandonment_id,
    c.user_id,
    c.user_session,
    c.product_id,
    c.brand,
    c.price,
    c.cart_time,
    coalesce(c.loyalty_tier, 'Member') as loyalty_tier,
    coalesce(c.acquisition_channel, 'Organic') as acquisition_channel
FROM cart_events c
LEFT JOIN purchase_events p
  ON c.user_session = p.user_session
  AND c.product_id = p.product_id
  AND p.purchase_time >= c.cart_time
WHERE c.rn = 1
  AND p.user_session IS NULL
