WITH src_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server__events') }}
    ),

type AS (
    SELECT
     md5(event_type) as event_type_id,
     event_type
)

SELECT * FROM type