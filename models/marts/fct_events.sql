WITH events AS (
    SELECT * 
    FROM {{ref('stg_sql_server__events')}}
),

final AS (
    SELECT
    {{dbt_utils.generate_surrogate_key(['event_id'])}} as event_key,
    {{dbt_utils.generate_surrogate_key(['user_id'])}} as user_key,
    {{dbt_utils.generate_surrogate_key(['created_at_utc'])}} as time_key,
    IFF(product_id IS NULL, NULL,{{dbt_utils.generate_surrogate_key(['product_id'])}}) as product_key,
    IFF(order_id IS NULL, NULL,{{dbt_utils.generate_surrogate_key(['order_id'])}}) as order_key,
    session_id,
    page_url,
    event_type_id
    FROM events
)

SELECT * FROM final