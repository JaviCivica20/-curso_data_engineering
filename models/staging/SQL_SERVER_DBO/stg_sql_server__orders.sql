WITH src_orders AS (
    SELECT *
    FROM {{ ref("base_sql_server__orders") }}
),

renamed_casted AS (
    SELECT
        order_id,
        convert_timezone('UTC',created_at) as created_at_utc,
        user_id,
        address_id,
        order_total as order_total_dollars,
        order_cost as order_cost_dollars,
        shipping_cost as shipping_cost_dollars,
        CASE 
            WHEN status LIKE 'preparing' then 0
            WHEN status LIKE 'shipped' then 1
            WHEN status LIKE 'delivered' then 2
        END AS status_id,
        CASE
            WHEN promo_id = '' THEN md5('no promo')
            ELSE md5(promo_id)
        END AS promo_id,
        coalesce(_fivetran_deleted, false) AS date_deleted,
        convert_timezone('UTC', _fivetran_synced) AS date_load
    FROM src_orders
)

SELECT * FROM renamed_casted
