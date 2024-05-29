WITH orders AS (
    SELECT * 
    FROM {{ref('stg_sql_server__orders')}}
),

addresses AS (
    SELECT 
        address_id
    FROM {{ref('dim_addresses')}}
),

products AS (
    SELECT 
        product_id
    FROM {{ref('dim_products')}}
),

promos AS (
    SELECT 
        promo_id
    FROM {{ref('dim_promos')}}
),

users AS (
    SELECT 
        user_id
    FROM {{ref('dim_users')}}
),

time AS (
    SELECT 
        time_id
    FROM {{ref('dim_time')}}
),

order_items AS (
    SELECT
        quantity
    FROM {{ref('stg_sql_server__order_items')}}
),

joined_data AS (
    SELECT
        b.quantity,
        b.product_id,
        a.order_cost,
        a.order_total,
        a.shipping_cost
    FROM orders a 
    LEFT JOIN order_items b 
    ON a.product_id = b.product_id
),

final AS (
    SELECT
        user_id,
        address_id,
        product_id,
        promo_id,
        time_id,
        quantity,
        order_cost,
        order_total,
        shipping_cost
    FROM joined_data jd
    LEFT JOIN
    {{ ref('dim_products') }} dp ON jd.PRODUCT_ID = dp.PRODUCT_ID
    LEFT JOIN
    {{ ref('dim_users') }} du ON jd.USER_ID = du.USER_ID
    LEFT JOIN
    {{ ref('dim_addresses') }} da ON jd.ADDRESS_ID = da.ADDRESS_ID
    LEFT JOIN
    {{ ref('dim_promos') }} dpromo ON jd.PROMO_ID = dpromo.PROMO_ID
    LEFT JOIN
    {{ ref('dim_time') }} dt ON jd.CREATED_AT::DATE = dt.date 

)

SELECT * FROM final