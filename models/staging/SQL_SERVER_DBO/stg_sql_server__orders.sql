
WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ), 

renamed_casted AS (
    SELECT
          order_id
        , shipping_service
        , shipping_cost
        , address_id
        , created_at
        , CASE WHEN promo_id = '' then md5('sin promocion')
            ELSE md5(promo_id) END AS promo_id
        , estimated_delivery_at
        , order_cost
        , user_id
        , order_total
        , delivered_at
        , tracking_id
        , status
        , _fivetran_deleted AS date_deleted
        , _fivetran_synced AS date_load
    FROM src_orders
    )

SELECT * FROM renamed_casted