WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ), 

tracking AS (
    SELECT  
        tracking_id,
        shipping_service,
        delivered_at,
        estimated_delivery_at

    FROM src_orders
    WHERE estimated_delivery_at IS NOT NULL
)

SELECT * FROM tracking