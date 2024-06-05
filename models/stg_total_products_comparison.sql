WITH order_items AS (
    SELECT 
        product_id,
        extract(month from o.created_at_utc) AS month,
        sum(quantity) as products_total_sales
        --SUM(quantity) OVER(PARTITION BY product_id) as products_total_sales
    FROM {{ref('stg_sql_server__order_items')}} oi
    JOIN {{ref('stg_sql_server__orders')}} o
    ON oi.order_id = o.order_id
    GROUP BY 2, 1
),

budget AS (
    SELECT DISTINCT
        product_id,
        month,
        --SUM(quantity)OVER(PARTITION BY product_id) AS total_products,
        sum(quantity) AS budget_total_products
    FROM {{ ref('stg_google_sheets__budget') }}
    GROUP BY 
 ),

final AS (
    SELECT DISTINCT
        a.product_id,
        total_products as budget_total_products,
        a.products_total_sales,
        b.month,
        a.products_total_sales - budget_total_products AS sales_prevision
    FROM order_items a 
    JOIN budget b 
    ON a.product_id = b.product_id
    --GROUP BY b.month, a.product_id, budget_total_products, a.products_total_sales, sales_prevision
)

SELECT * FROM final 