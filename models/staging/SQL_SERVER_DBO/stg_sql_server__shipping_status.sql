WITH src_orders AS (
    SELECT status 
    FROM {{ref("stg_sql_server__orders")}}
    ), 

status as (
SELECT DISTINCT
    CASE 
        WHEN status LIKE 'preparing' then 0
        WHEN status LIKE 'shipped' then 1
        WHEN status LIKE 'delivered' then 2
    END AS status_id,
    status

FROM src_orders
ORDER BY status_id
)

SELECT * FROM status


