WITH src_invent AS (
    SELECT * 
    FROM {{ref('stg_sql_server__users')}}
    ),

addresses as (
    SELECT * 
    FROM {{ref('stg_sql_server__addresses')}}
),

final as (
    SELECT
        user_id,
        address
FROM src_invent
JOIN addresses
ON src_invent.address_id = addresses.address_id
)

SELECT * FROM final

