WITH order_items AS (
    SELECT 
        product_id,
        extract(month from o.created_at_utc) AS month,
        sum(quantity) as products_total_sales
    FROM {{ref('stg_sql_server__order_items')}} oi
    JOIN {{ref('stg_sql_server__orders')}} o
    ON oi.order_id = o.order_id
    GROUP BY product_id, month
),

budget AS (
    SELECT
        product_id,
        extract(month from month) AS month,
        sum(quantity) AS budget_total_products
    FROM {{ ref('stg_google_sheets__budget') }}
    GROUP BY product_id, month
 ),

final AS (
    SELECT
        b.product_id,
        b.month,
        b.budget_total_products,

        CASE 
            WHEN b.month = oi.month THEN oi.products_total_sales
            ELSE NULL
        END AS product_sales_month,
        
        CASE
            WHEN b.month = oi.month AND oi.products_total_sales - b.budget_total_products > 0 THEN CONCAT('+', (oi.products_total_sales - b.budget_total_products)::STRING)
            WHEN b.month = oi.month AND oi.products_total_sales - b.budget_total_products <= 0 THEN (oi.products_total_sales - b.budget_total_products)::STRING
            ELSE NULL
        END AS prevision_difference
        
    FROM order_items oi 
    JOIN budget b 
    ON oi.product_id = b.product_id
    ORDER BY month
)

SELECT * FROM final 