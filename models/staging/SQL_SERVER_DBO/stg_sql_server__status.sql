WITH src_promos AS (
    SELECT status_id
    FROM {{ref('stg_sql_server__promos')}}
    ),

dim_status AS (
    SELECT DISTINCT
        status_id,
        CASE   
            WHEN status_id = 1 then 'active'
            WHEN status_id = 0 then 'inactive'
            END status,
    FROM src_promos
)

SELECT * FROM dim_status
