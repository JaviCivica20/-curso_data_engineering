WITH src_orders AS (
    SELECT * 
    FROM {{ref("base_sql_server__orders")}}
    ), 

tracking AS (
    SELECT  
        tracking_id,
        shipping_service,
        md5(shipping_service) as shipping_service_id,
        convert_timezone('UTC', delivered_at) as delivered_at_utc,
        convert_timezone('UTC', estimated_delivery_at) as estimated_delivery_at_utc

    FROM src_orders
    WHERE estimated_delivery_at IS NOT NULL
)

SELECT * FROM tracking