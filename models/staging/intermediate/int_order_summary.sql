WITH order_items AS (
    SELECT
        order_id,
        sum(quantity) as total_quantity
    FROM {{ ref('stg_sql_server__order_items') }}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.shipping_cost_dollars,
    oi.total_quantity
FROM {{ ref('stg_sql_server__orders') }} o
join order_items oi on o.order_id = oi.order_id