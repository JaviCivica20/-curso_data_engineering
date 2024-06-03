WITH src_orders AS (
    SELECT * 
    FROM {{ref("base_sql_server__orders")}}
    ), 

shipping AS (
    SELECT DISTINCT
        {{dbt_utils.generate_surrogate_key(['shipping_service'])}} as shipping_service_id, 
        shipping_service,
        shipping_cost

    FROM src_orders
    
)

SELECT * FROM shipping