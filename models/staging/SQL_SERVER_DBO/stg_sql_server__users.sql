WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

user_orders AS (
    SELECT  
        user_id,
        count(*) as total_orders
    FROM {{ source('sql_server_dbo', 'orders') }}
    GROUP BY user_id
),

renamed_casted AS (
    SELECT
          a.user_id,
          first_name,
          last_name,
          phone_number,
          email,
          b.total_orders,
          address_id,
          convert_timezone('UTC',updated_at) as updated_at_utc,
          convert_timezone('UTC',created_at) as created_at_utc,
          coalesce(_fivetran_deleted, false) AS date_deleted,
          convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_users a
    LEFT JOIN user_orders b
    ON a.user_id = b.user_id
    )

SELECT * FROM renamed_casted