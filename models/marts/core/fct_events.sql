WITH events AS (
    SELECT *
    FROM {{ ref('stg_sql_server__events') }}
),

final AS (
    SELECT
        event_id,
        page_url,
        event_type_id,
        user_id,
        product_id,
        session_id,
        created_at_utc,
        order_id
    FROM events
)

SELECT * FROM final
