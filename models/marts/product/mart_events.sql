
WITH events AS (
    SELECT * 
    FROM {{ref('fct_events')}}
),

events_sum AS (
    SELECT *
    FROM {{ref('int_events_sum')}}

),

users AS (
    SELECT * 
    FROM {{ref('dim_users')}}
),

joined AS (
    SELECT DISTINCT
        a.user_id,
        MIN(a.created_at_utc) AS first_event,
        MAX(a.created_at_utc) AS last_event,
        COUNT(DISTINCT a.session_id) AS sessions,
        COUNT(a.page_url) AS pages_views,
        DATEDIFF(minute, MIN(a.created_at_utc), MAX(a.created_at_utc)) AS total_sessions_minutes,
        b.checkout_amount,
        b.package_shipped_amount,
        b.add_to_cart_amount,
        b.page_view_amount
    FROM users u
    JOIN events a
    ON u.user_id = a.user_id
    JOIN events_sum b
    ON u.user_id = b.user_id
    GROUP BY a.user_id, b.checkout_amount, b.package_shipped_amount, b.add_to_cart_amount, b.page_view_amount
    ORDER BY sessions DESC
)

SELECT * FROM joined