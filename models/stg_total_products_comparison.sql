WITH order_items AS (
    SELECT 
        product_id,
        extract(month from o.created_at_utc) AS month,
        sum(quantity) as products_total_sales
        --SUM(quantity) OVER(PARTITION BY product_id) as products_total_sales
    FROM {{ref('stg_sql_server__order_items')}} oi
    JOIN {{ref('stg_sql_server__orders')}} o
    ON oi.order_id = o.order_id
    WHERE month = 2
    GROUP BY product_id, month
),

budget AS (
    SELECT
        product_id,
        month,
        --SUM(quantity)OVER(PARTITION BY product_id) AS total_products,
        sum(quantity) AS budget_total_products
    FROM {{ ref('stg_google_sheets__budget') }}
    GROUP BY product_id, month
 ),

final AS (
    SELECT DISTINCT
        oi.product_id,
        b.budget_total_products,
        CASE 
            WHEN oi.month = 2 THEN oi.products_total_sales
            WHEN b.month = 3 THEN oi.products_total_sales = NULL
        END AS product_sales_month,
        b.month,
        oi.products_total_sales - b.budget_total_products AS sales_prevision
    FROM order_items oi 
    JOIN budget b 
    ON oi.product_id = b.product_id
    ORDER BY month
    --WHERE b.month = oi.month
    --GROUP BY b.month, a.product_id, budget_total_products, a.products_total_sales, sales_prevision
)

SELECT * FROM final 