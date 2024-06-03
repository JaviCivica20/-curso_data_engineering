
WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}
 ),

renamed_casted AS (
    SELECT
         _row,
         product_id,
        quantity,
        extract(month from month) as month,
        convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_budget
    )

SELECT * FROM renamed_casted

