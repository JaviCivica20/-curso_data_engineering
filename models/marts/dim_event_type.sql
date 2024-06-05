WITH src_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server__event_type') }}
    ),

type AS (
    SELECT DISTINCT
     event_type_id,
     event_type
     FROM src_events
)

SELECT * FROM type
