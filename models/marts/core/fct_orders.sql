WITH orders AS (
    SELECT * 
    FROM {{ref('stg_sql_server__orders')}}
),

order_items AS (
    SELECT * 
    FROM {{ref('stg_sql_server__order_items')}}
),

final AS (
    SELECT DISTINCT
        a.order_id,
        address_id,
        promo_id,
        user_id,
        created_at_utc,
        order_cost_dollars,
        order_total_dollars,
        shipping_cost_dollars,
        order_total_dollars - (shipping_cost_dollars + order_cost_dollars) AS discount
    FROM orders a 
    JOIN order_items b 
    ON a.order_id = b.order_id   
)

SELECT * FROM final