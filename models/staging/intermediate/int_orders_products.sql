WITH order_items AS (
    SELECT * 
    FROM {{ref('stg_sql_server__order_items')}}
),

order_summary AS (
    SELECT * 
    FROM {{ ref('int_order_summary') }}
),

products AS (
    SELECT *
    FROM {{ ref('stg_sql_server__products') }}
),

final AS (
    SELECT
        b.order_id,
        b.product_id,
        --c.name,
        b.quantity,
        c.price_dollars,
        c.price_dollars * b.quantity AS total_price,
        a.shipping_cost_dollars/total_quantity AS shipping_cost_per_product
    FROM order_summary a 
    JOIN order_items b 
    ON a.order_id = b.order_id
    JOIN products c 
    ON b.product_id = c.product_id
    ORDER BY b.order_id
)

SELECT * FROM final