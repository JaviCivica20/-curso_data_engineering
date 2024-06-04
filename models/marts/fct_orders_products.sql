WITH order_items AS (
    SELECT
        order_id,
        product_id,
        sum(quantity) as total_quantity
    FROM {{ ref('stg_sql_server__order_items') }}
    GROUP BY order_id, product_id
),

products AS (
    SELECT *
    FROM {{ ref('stg_sql_server__products') }}
),

orders AS (
    SELECT 
        user_id,
        order_id,
        address_id,
        promo_id,
        created_at_utc,
        status_id,
        order_total_dollars,
        order_cost_dollars,
        shipping_cost_dollars,
        t.shipping_service_id
    FROM {{ ref('stg_sql_server__orders') }} o
    JOIN {{ ref('stg_sql_server__tracking') }} t
    ON o.tracking_id = t.tracking_id
),

final AS (
    SELECT
        ROW_NUMBER()OVER(PARTITION BY oi.order_id ORDER BY oi.product_id) AS _ROW,
        oi.order_id,
        o.address_id,
        o.user_id,
        o.promo_id,
        {{dbt_utils.generate_surrogate_key(['o.created_at_utc'])}} AS time_id,
        o.shipping_service_id,
        oi.product_id,
        o.status_id AS shipping_status_id,
        oi.total_quantity,
        ROUND(p.price_dollars * oi.total_quantity, 2) AS total_price_per_product,
        o.order_total_dollars,
        o.order_cost_dollars,
        o.shipping_cost_dollars,
        (o.order_total_dollars - (o.shipping_cost_dollars + o.order_cost_dollars))::int AS discount
    FROM orders o 
    JOIN order_items oi
    ON o.order_id = oi.order_id
    JOIN products p 
    ON oi.product_id = p.product_id
)

SELECT * FROM final