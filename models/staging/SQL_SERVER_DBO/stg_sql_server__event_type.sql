WITH src_events AS (
    SELECT * 
    FROM {{ ref('base_sql_server__events') }}
    ),

type AS (
    SELECT DISTINCT
     md5(event_type),
     event_type
     FROM src_events
)

SELECT * FROM type
