WITH orders AS (
    SELECT * 
    FROM {{ref('stg_sql_server__orders')}}
),

products AS (
    SELECT * 
    FROM {{ref('stg_sql_server__order_items')}}
),


final AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['a.order_id', 'product_id'])}} as sales_key,
        {{dbt_utils.generate_surrogate_key(['a.address_id'])}} as address_key,
        {{dbt_utils.generate_surrogate_key(['product_id'])}} as product_key,
        {{dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_key,
        {{dbt_utils.generate_surrogate_key(['created_at_utc'])}} as time_key, -- Mirar esto
        {{dbt_utils.generate_surrogate_key(['user_id'])}} as user_key,
        order_cost_dollars,
        order_total_dollars,
        shipping_cost_dollars
    FROM orders a 
    JOIN products b 
    ON a.order_id = b.order_id
)

SELECT * FROM final