{% snapshot snap_users_loyalty %}

{{
    config(
        target_schema='gold',
        unique_key='user_id',
        strategy='check',
        check_cols=['loyalty_tier'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    user_id,
    loyalty_tier,
    crm_loyalty_tier,
    total_orders,
    total_spend_usd,
    avg_order_value,
    CURRENT_TIMESTAMP AS snapshot_at
FROM {{ ref('dim_users_loyalty') }}

{% endsnapshot %}
