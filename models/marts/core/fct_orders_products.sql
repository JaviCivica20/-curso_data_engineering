WITH order_items AS (
    SELECT
        order_id,
        product_id,
        quantity,
        COUNT(product_id)OVER(PARTITION BY order_id) AS pr
    FROM {{ ref('stg_sql_server__order_items') }}
),

products AS (
    SELECT *
    FROM {{ ref('stg_sql_server__products') }}
),

orders AS (
    SELECT *
    FROM {{ ref('stg_sql_server__orders') }} 
),

final AS (
    SELECT
        --ROW_NUMBER()OVER(PARTITION BY oi.order_id ORDER BY oi.product_id) AS _ROW,
        o.order_id,
        o.address_id,
        o.user_id,
        o.promo_id,
        o.created_at_utc,
        o.shipping_service_id,
        o.tracking_id,
        oi.product_id,
        o.status_id AS shipping_status_id,
        oi.quantity AS product_quantity,
        ROUND(p.price_dollars * oi.quantity, 2) AS total_price_per_product,
        o.order_total_dollars/pr AS order_total,
        o.order_cost_dollars/pr AS order_cost,
        o.shipping_cost_dollars/pr AS shipping_cost,
        ROUND(((o.order_total_dollars - (o.shipping_cost_dollars + o.order_cost_dollars))/pr),2) AS discount
    FROM orders o 
    LEFT JOIN order_items oi
    ON o.order_id = oi.order_id
    LEFT JOIN products p 
    ON oi.product_id = p.product_id
    ORDER BY order_id
)

SELECT * FROM final