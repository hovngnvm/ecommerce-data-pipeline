{{ config(materialized='table') }}

WITH user_events AS (
    SELECT
        user_id,
        COUNT(DISTINCT user_session) AS total_sessions,
        MIN(event_time) AS first_active_time,
        MAX(event_time) AS last_active_time,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_orders,
        COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN price END), 0.0) AS total_spend_usd,
        MIN(CASE WHEN event_type = 'purchase' THEN event_time END) AS first_purchase_at,
        MAX(CASE WHEN event_type = 'purchase' THEN event_time END) AS last_purchase_at
    FROM {{ ref('stg_events') }}
    WHERE user_id IS NOT NULL
    GROUP BY user_id
)

SELECT
    u.user_id,
    -- Dynamic Tier Engine based on aggregated spend and order volume
    CASE
        WHEN u.total_spend_usd >= 2000.0 OR u.total_orders >= 10 THEN 'Platinum'
        WHEN u.total_spend_usd >= 800.0  OR u.total_orders >= 5  THEN 'Gold'
        WHEN u.total_spend_usd >= 200.0  OR u.total_orders >= 2  THEN 'Silver'
        ELSE 'Member'
    END AS loyalty_tier,
    COALESCE(l.loyalty_tier, 'Member') AS crm_loyalty_tier,
    l.signup_date,
    COALESCE(l.acquisition_channel, 'Organic') AS acquisition_channel,
    u.total_sessions,
    u.total_orders,
    ROUND(u.total_spend_usd, 2) AS total_spend_usd,
    CASE
        WHEN u.total_orders > 0 THEN ROUND(u.total_spend_usd / u.total_orders, 2)
        ELSE 0.0
    END AS avg_order_value,
    u.first_active_time,
    u.last_active_time,
    u.first_purchase_at,
    u.last_purchase_at
FROM user_events u
LEFT JOIN {{ ref('stg_user_loyalty') }} l
  ON u.user_id = l.user_id
