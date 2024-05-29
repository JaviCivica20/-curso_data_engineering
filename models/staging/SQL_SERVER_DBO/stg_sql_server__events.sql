
WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
          event_id,
          page_url,
          event_type,
          md5(event_type) as event_type_id,
          user_id,
          NULLIF( product_id , '' ) AS product_id ,
          session_id,
          convert_timezone('UTC', created_at),
          NULLIF(order_id, '') AS order_id,
          coalesce(_fivetran_deleted, false) AS date_deleted,
          convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_events
    )

SELECT * FROM renamed_casted