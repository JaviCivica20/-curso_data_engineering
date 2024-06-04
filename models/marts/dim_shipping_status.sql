WITH src_orders AS (
    SELECT *
    FROM {{ref("stg_sql_server__shipping_status")}}
    ), 

final as (
SELECT 
    status_id AS shipping_status_id,
    status
FROM src_orders
ORDER BY status_id
)

SELECT * FROM final