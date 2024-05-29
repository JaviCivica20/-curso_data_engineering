WITH src_orders AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'orders') }}
),

renamed_casted AS (
    SELECT
        order_id,
        shipping_service, 
        created_at,
        user_id,
        address_id,
        order_total,
        estimated_delivery_at,
        order_cost,
        shipping_cost,
        status,
        delivered_at,
        tracking_id,
        promo_id,
        _fivetran_deleted,
        _fivetran_synced
    FROM src_orders
)

SELECT * FROM renamed_casted
