
WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
          address_id
        , zipcode
        , country
        , address
        , state
        , coalesce(_fivetran_deleted, false) AS date_deleted
        , convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_addresses
    )

SELECT * FROM renamed_casted