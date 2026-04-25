{{ config(materialized='table') }}

WITH ranked_snapshots AS (
    SELECT
        user_id,
        loyalty_tier AS current_tier,
        crm_loyalty_tier,
        total_orders,
        total_spend_usd,
        dbt_valid_from,
        dbt_valid_to,
        LAG(loyalty_tier) OVER (
            PARTITION BY user_id
            ORDER BY dbt_valid_from
        ) AS previous_tier,
        LAG(dbt_valid_from) OVER (
            PARTITION BY user_id
            ORDER BY dbt_valid_from
        ) AS previous_tier_start_at
    FROM {{ ref('snap_users_loyalty') }}
)

SELECT
    md5(concat(user_id::text, '-', current_tier, '-', COALESCE(dbt_valid_from::text, '1970-01-01'))) AS progression_id,
    user_id,
    COALESCE(previous_tier, 'Member') AS previous_tier,
    current_tier,
    CASE
        WHEN previous_tier IS NULL THEN 'INITIAL'
        WHEN previous_tier = 'Member' AND current_tier IN ('Silver', 'Gold', 'Platinum') THEN 'UPGRADE'
        WHEN previous_tier = 'Silver' AND current_tier IN ('Gold', 'Platinum') THEN 'UPGRADE'
        WHEN previous_tier = 'Gold' AND current_tier = 'Platinum' THEN 'UPGRADE'
        WHEN previous_tier = current_tier THEN 'RETAIN'
        ELSE 'DOWNGRADE'
    END AS transition_type,
    CASE
        WHEN previous_tier_start_at IS NOT NULL AND dbt_valid_from IS NOT NULL THEN
            DATE_DIFF('day', CAST(previous_tier_start_at AS DATE), CAST(dbt_valid_from AS DATE))
        ELSE 0
    END AS days_in_previous_tier,
    dbt_valid_from AS transitioned_at,
    total_orders,
    total_spend_usd
FROM ranked_snapshots
