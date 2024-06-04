WITH users AS (
    SELECT * 
    FROM {{ref('stg_sql_server__users')}}
),

final AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['user_id'])}} as user_id,
        first_name,
        last_name,
        phone_number,
        email
    FROM users
)

SELECT * FROM final