
WITH src_budget AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets__budget') }}
    ),

renamed_casted AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['_row'])}} as budget_id,
        product_id,
        quantity,
        month       
    FROM src_budget
    --GROUP BY month
    )

SELECT * FROM renamed_casted