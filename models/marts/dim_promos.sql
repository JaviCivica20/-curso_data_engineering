WITH promos AS (
    SELECT * 
    FROM {{ref('stg_sql_server__promos')}}
),

final AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_key,
        promo_id,
        promo_name,
        discount_dollars,
        status_id AS status
    FROM promos
)

SELECT * FROM final