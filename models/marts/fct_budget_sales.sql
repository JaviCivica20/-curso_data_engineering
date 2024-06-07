WITH stg_budget AS (
    SELECT 
        LEFT(month,7) AS date,
        product_id,
        sum(quantity) AS budget_quantity
    FROM {{ref('stg_google_sheets__budget')}} 
    GROUP BY 1,2
),

fct_orders_products AS (
    SELECT
        LEFT(created_at_utc,7) AS created_at,
        product_id,
        sum(product_quantity) AS sales_quantity
    FROM {{ref('fct_orders_products')}}
    GROUP BY 1,2
),

joined AS (
    SELECT
        b.date,
        b.product_id,
        budget_quantity,
        sales_quantity,
        sales_quantity - budget_quantity AS prevision_difference
    FROM stg_budget b 
    LEFT JOIN fct_orders_products op ON b.product_id = op.product_id AND b.date = op.created_at
    ORDER BY b.date

)

SELECT * FROM joined