WITH promos AS (
    SELECT * 
    FROM {{ref('stg_sql_server__promos')}}
),

final AS (
    SELECT
        promo_id,
        promo_name,
        discount_dollars,
        --status_id AS status
    FROM promos
)

SELECT * FROM final