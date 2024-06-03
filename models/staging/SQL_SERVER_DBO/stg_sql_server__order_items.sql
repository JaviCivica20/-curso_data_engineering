
WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

renamed_casted AS (
    SELECT
        order_id,
        product_id,
        quantity,
        coalesce(_fivetran_deleted, false) AS date_deleted,
        convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM   rc_order_items
    )

SELECT * FROM renamed_casted