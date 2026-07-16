{{ config(materialized='table') }}

SELECT
    user_id,
    COUNT(DISTINCT user_session) AS total_sessions,
    MIN(event_time) AS first_seen,
    MAX(event_time) AS last_seen
FROM {{ source('db_silver', 'ecommerce_events') }}
WHERE user_id IS NOT NULL
GROUP BY user_id