WITH order_items AS (
    SELECT * 
    FROM {{ref('stg_sql_server__order_items')}}
),

final AS (
    SELECT
        product_id,
        SUM(quantity) OVER(PARTITION BY product_id) as products_total_sales
    FROM order_items
)

SELECT * FROM final