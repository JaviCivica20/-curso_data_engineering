WITH src_tracking AS (
    SELECT shipping_service
    FROM {{ ref('base_sql_server__orders') }}
),

shipping_service AS (
    SELECT DISTINCT
        {{dbt_utils.generate_surrogate_key(['shipping_service'])}} AS shipping_service_id,
        shipping_service
    FROM src_tracking
    WHERE shipping_service !=''
)

SELECT * FROM shipping_service