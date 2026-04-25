{{ config(materialized='view') }}

SELECT
    TRY_CAST(user_id AS INTEGER) as user_id,
    CASE
        WHEN loyalty_tier IN ('Platinum', 'VIP', 'Diamond') THEN 'Platinum'
        WHEN loyalty_tier = 'Gold' THEN 'Gold'
        WHEN loyalty_tier = 'Silver' THEN 'Silver'
        ELSE 'Member'
    END AS loyalty_tier,
    TRY_CAST(signup_date AS DATE) as signup_date,
    COALESCE(acquisition_channel, 'Organic') as acquisition_channel
FROM {{ source('db_crm', 'user_loyalty') }}
WHERE user_id IS NOT NULL
