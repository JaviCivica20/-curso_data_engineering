WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
        product_id,
        price as price_dollars,
        name,
        inventory,
        coalesce(_fivetran_deleted, false) AS date_deleted,
        convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_products
    )

SELECT * FROM renamed_casted