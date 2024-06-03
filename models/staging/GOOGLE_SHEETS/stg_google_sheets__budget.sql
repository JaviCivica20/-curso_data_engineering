{{ config(
    materialized='incremental',
    unique_key = 'budget_id'
    ) 
    }}

WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}
 ),

renamed_casted AS (
    SELECT
          {{dbt_utils.generate_surrogate_key(['_row'])}} as budget_id,
         product_id
        , quantity
        , extract(month from month) as month
        , convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_budget
    )

SELECT * FROM renamed_casted

{% if is_incremental() %}

	  WHERE date_load > (SELECT MAX(date_load) FROM {{ this }} )

{% endif %}