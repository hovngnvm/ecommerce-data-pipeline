{{ config(
    materialized='incremental',
    unique_key='sale_id'
) }}

SELECT
    md5(concat(event_time::text, '-', user_id::text, '-', product_id::text, '-', user_session)) as sale_id,
    event_time,
    user_id,
    product_id,
    brand,
    price,
    user_session,
    loyalty_tier,
    acquisition_channel
FROM {{ ref('stg_events') }}
WHERE event_type = 'purchase'

{% if is_incremental() %}
-- Only process data within a 2-day lookback window of the latest processed date to handle late arrivals/reruns
AND event_time >= (SELECT MAX(event_time) - INTERVAL '2 days' FROM {{ this }})
{% endif %}

