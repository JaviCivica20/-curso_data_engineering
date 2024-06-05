WITH promos_status AS (
    SELECT *
    FROM {{ref('stg_sql_server__promo_status')}}
    ),

status AS (
    SELECT DISTINCT
        status_id,
        status
    FROM promos_status
)

SELECT * FROM status