WITH src_tracking AS (
    SELECT *
    FROM {{ ref('stg_sql_server__tracking') }}
),