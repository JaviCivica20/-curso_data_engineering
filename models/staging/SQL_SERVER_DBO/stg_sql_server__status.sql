WITH src_promos AS (
    SELECT status
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

dim_status AS (
    SELECT DISTINCT
        CASE   
            WHEN status LIKE 'active' then 1
            WHEN status LIKE 'inactive' then 0
            END AS status_id,
        status

    FROM src_promos
)

SELECT * FROM dim_status
