WITH events AS (
    SELECT * 
    FROM {{ref('stg_sql_server__events')}}
),

final AS (
    SELECT
    {{dbt_utils.generate_surrogate_key(['event_id'])}} as event_id,
    {{dbt_utils.generate_surrogate_key(['user_id'])}} as user_id,
    {{dbt_utils.generate_surrogate_key(['created_at_utc'])}} as time_id,
    IFF(product_id IS NULL, NULL,{{dbt_utils.generate_surrogate_key(['product_id'])}}) as product_id,
    IFF(order_id IS NULL, NULL,{{dbt_utils.generate_surrogate_key(['order_id'])}}) as order_id,
    session_id,
    page_url,
    event_type_id
    FROM events
)

SELECT * FROM final