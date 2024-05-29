WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

user_orders AS (
    SELECT  
        user_id,
        count(*) as total_orders
    FROM {{ ref('base_sql_server__orders') }}
    GROUP BY user_id
),

renamed_casted AS (
    SELECT
          a.user_id,
          first_name,
          last_name,
          phone_number,
          email,
          coalesce(b.total_orders, 0),
          address_id,
          --coalesce (regexp_like(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')= true,false) as is_valid_email_address,
          convert_timezone('UTC',updated_at) as updated_at_utc,
          convert_timezone('UTC',created_at) as created_at_utc,
          coalesce(_fivetran_deleted, false) AS date_deleted,
          convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_users a
    LEFT JOIN user_orders b
    ON a.user_id = b.user_id
    )

SELECT * FROM renamed_casted