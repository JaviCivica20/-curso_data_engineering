
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
    FROM src_order_items
    ORDER BY order_id
    )

SELECT * FROM renamed_casted