{{ config(materialized='view') }}

SELECT
    TRY_CAST(user_id AS INTEGER) as user_id,
    COALESCE(loyalty_tier, 'Regular') as loyalty_tier,
    TRY_CAST(signup_date AS DATE) as signup_date,
    COALESCE(acquisition_channel, 'Organic') as acquisition_channel
FROM {{ source('db_crm', 'user_loyalty') }}
WHERE user_id IS NOT NULL
