
WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
          address_id::varchar(50)
        , zipcode::number(5,0)
        , country::varchar(50)
        , address::varchar(50)
        , state::varchar(50)
        , _fivetran_deleted AS date_deleted
        , _fivetran_synced AS date_load
    FROM src_addresses
    )

SELECT * FROM renamed_casted