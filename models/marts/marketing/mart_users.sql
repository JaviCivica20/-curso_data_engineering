WITH users AS (
    SELECT * 
    FROM {{ref('dim_users')}}
),

address AS (
    SELECT *
    FROM {{ref('dim_addresses')}}
),

users_aggregations AS (
    SELECT * 
    FROM {{ref('int_users_aggregations')}}
),

order_products AS (
    SELECT 
        user_id,
        address_id
    FROM {{ref('fct_orders_products')}}
),

final AS (
    SELECT DISTINCT
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        u.created_at_utc,
        u.updated_at_utc,
        a.address,
        a.zipcode,
        a.state,
        a.country,
        ua.total_orders,
        ua.total_orders_cost,
        ua.total_shipping_cost,
        ua.total_products,
        ua.different_products,
        ua.total_discounts
    FROM users u
    LEFT JOIN users_aggregations ua ON ua.user_id = u.user_id
    LEFT JOIN order_products op ON op.user_id = ua.user_id
    LEFT JOIN address a ON op.address_id = a.address_id
)

SELECT * FROM final