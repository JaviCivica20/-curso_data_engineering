WITH src_tracking AS (
    SELECT *
    FROM {{ ref('stg_sql_server__tracking') }}
),

shipping_service AS (
    SELECT DISTINCT
        shipping_service_id,
        shipping_service
    FROM src_tracking
)

SELECT * FROM shipping_service