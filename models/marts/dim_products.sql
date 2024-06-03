WITH products AS (
    SELECT * 
    FROM {{ref('stg_sql_server__products')}}
),

final AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['product_id'])}} as product_id,
        name,
        price_dollars
    FROM products
)

SELECT * FROM final