
WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),


promos AS (    
    SELECT promo_id
    FROM {{ source('sql_server_dbo', 'promos') }}
),

renamed_casted AS (
    SELECT
          order_id
        , shipping_service
        , shipping_cost
        , address_id
        , created_at
        , CASE WHEN b.promo_id is null then md5('Sin promocion')
            ELSE md5(b.promo_id) END AS promo_id
        , estimated_delivery_at
        , order_cost
        , user_id
        , order_total
        , delivered_at
        , tracking_id
        , status
        , _fivetran_deleted AS date_deleted
        , _fivetran_synced AS date_load
    FROM src_orders a
    LEFT JOIN promos b
    ON a.promo_id = b.promo_id
    )

SELECT * FROM renamed_casted