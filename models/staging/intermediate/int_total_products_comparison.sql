WITH order_items AS (
    SELECT 
        product_id,
        SUM(quantity) OVER(PARTITION BY product_id) as products_total_sales
    FROM {{ref('stg_sql_server__order_items')}}
),

budget AS (
    SELECT DISTINCT
        product_id,
        SUM(quantity)OVER(PARTITION BY product_id) AS total_products,
    FROM {{ ref('stg_google_sheets__budget') }}
 ),

final AS (
    SELECT DISTINCT
        a.product_id,
        total_products as budget_total_products,
        a.products_total_sales
    FROM order_items a 
    JOIN budget b 
    ON a.product_id = b.product_id
)

SELECT * FROM final 