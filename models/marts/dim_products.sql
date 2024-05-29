WITH products AS (
    SELECT * 
    FROM {{ref('stg_sql_server__products')}}
),

final AS (
    SELECT
        product_id,
        price_dollars,
        name
        --inventory
    FROM products
)

SELECT * FROM final