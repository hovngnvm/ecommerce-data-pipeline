{{ config(materialized='table') }}

SELECT
    u.user_id,
    l.loyalty_tier,
    l.signup_date,
    l.acquisition_channel,
    COUNT(DISTINCT u.user_session) AS total_sessions,
    MIN(u.event_time) AS first_active_time,
    MAX(u.event_time) AS last_active_time
FROM {{ source('db_silver', 'ecommerce_events') }} u
LEFT JOIN {{ source('db_crm', 'user_loyalty') }} l
ON u.user_id = l.user_id
GROUP BY u.user_id, l.loyalty_tier, l.signup_date, l.acquisition_channel
