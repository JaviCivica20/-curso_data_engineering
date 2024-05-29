WITH src_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server__events') }}
    ),

type AS (
    SELECT DISTINCT
     event_type_id,
     event_type
     FROM src_events
)

SELECT * FROM type