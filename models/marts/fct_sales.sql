WITH orders AS (
    SELECT * 
    FROM {{ref('stg_sql_server__orders')}}
),

order_items AS (
    SELECT * 
    FROM {{ref('stg_sql_server__order_items')}}
),

final AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['a.order_id', 'product_id'])}} as sales_id,
        {{dbt_utils.generate_surrogate_key(['address_id'])}} as address_id,
        {{dbt_utils.generate_surrogate_key(['product_id'])}} as product_id,
        {{dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_id,
        {{dbt_utils.generate_surrogate_key(['created_at_utc'])}} as time_id, 
        {{dbt_utils.generate_surrogate_key(['user_id'])}} as user_id,
        created_at_utc::date as created_at_utc,
        order_cost_dollars,
        order_total_dollars,
        shipping_cost_dollars,
    FROM orders a 
    JOIN order_items b 
    ON a.order_id = b.order_id   
)

SELECT * FROM final