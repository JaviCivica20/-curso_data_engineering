
WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ), 

renamed_casted AS (
    SELECT
          order_id,
        --, CASE WHEN shipping_service = '' then null
         --   ELSE shipping_service END AS shipping_service 
        --, shipping_cost
        address_id
        , created_at
        , CASE WHEN promo_id = '' then md5('no promo')
            ELSE md5(promo_id) END AS promo_id
        --, estimated_delivery_at
        , order_cost
        , user_id
        , order_total
        --, delivered_at
        --, CASE WHEN tracking_id = '' then null
        --    ELSE tracking_id END AS tracking_id
        , status
        , coalesce(_fivetran_deleted, false) AS date_deleted
        , convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_orders
    )

SELECT * FROM renamed_casted