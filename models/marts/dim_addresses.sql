WITH addresses AS (
    SELECT * 
    FROM {{ref('stg_sql_server__addresses')}}
),

final AS (
    SELECT
        address_id,
        zipcode,
        country,
        address,
        state
    FROM addresses
)

SELECT * FROM final