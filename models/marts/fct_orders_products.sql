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

orders AS (
    SELECT *
    FROM {{ ref('stg_sql_server__orders') }}
),

final AS (
    SELECT
        ROW_NUMBER()OVER(PARTITION BY os.order_id ORDER BY os.product_id) AS _ROW,
        os.order_id,
        os.product_id,
        o.address_id,
        os.total_quantity,
        p.price_dollars,
        p.price_dollars * os.total_quantity AS total_price_per_product
        --a.total_quantity
        --a.shipping_cost_dollars/total_quantity AS shipping_cost_per_product
    FROM orders o 
    JOIN order_summary os
    ON o.order_id = os.order_id
    JOIN products p 
    ON os.product_id = p.product_id
    --ORDER BY b.order_id
)

SELECT * FROM final