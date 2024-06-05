WITH src_tracking AS (
    SELECT *
    FROM {{ ref('stg_sql_server__shipping_service') }}
),

shipping_service AS (
    SELECT 
        shipping_service_id,
        shipping_service
    FROM src_tracking
)

SELECT * FROM shipping_service