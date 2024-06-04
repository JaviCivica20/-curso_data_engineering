WITH order_items AS (
    SELECT
        order_id,
        product_id,
        sum(quantity) as total_quantity
    FROM {{ ref('stg_sql_server__order_items') }}
    GROUP BY order_id, product_id
),

final AS (
    SELECT
    o.order_id,
    oi.product_id,
    oi.total_quantity
FROM {{ ref('stg_sql_server__orders') }} o
join order_items oi on o.order_id = oi.order_id
ORDER BY o.order_id
)

SELECT * FROM final 