WITH fct_budget AS (
    SELECT 
        year_month,
        product_id,
        sum(quantity) as budget_quantity
    FROM {{ref('fct_budget')}} b
    JOIN {{ref('dim_time')}} t
    ON b.month = t.date
    GROUP BY 1,2
),

fct_orders_products AS (
    SELECT
        year_month,
        product_id,
        sum(product_quantity) AS sales_quantity
    FROM {{ref('fct_orders_products')}} o 
    JOIN {{ref('dim_time')}} t
    ON o.created_at_utc = t.date
    GROUP BY 1,2
),

joined AS (
    SELECT
        b.year_month,
        b.product_id, 
        budget_quantity,
        sales_quantity,
        sales_quantity - budget_quantity AS prevision_difference
    FROM stg_budget b 
    LEFT JOIN fct_orders_products op ON b.product_id = op.product_id AND b.year_month = op.year_month
)
SELECT * FROM joined ORDER BY 1,2