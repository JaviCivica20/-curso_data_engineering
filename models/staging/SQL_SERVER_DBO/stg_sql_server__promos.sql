WITH src_promos AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
          md5(promo_id) as promo_id
        , promo_id AS promo_name
        , discount AS discount_dollars
        , CASE   
            WHEN status LIKE 'active' then 1
            WHEN status LIKE 'inactive' then 0
            END AS status_id
        , coalesce(_fivetran_deleted, false) AS date_deleted
        , convert_timezone('UTC',_fivetran_synced) AS date_load
    FROM src_promos a
    ),

new_row as (
    select
        md5('no promo') as promo_id,
        'no promo' as promo_name,
        0 as discount_dollars,  
        1 as status_id,  
        false as date_deleted, 
        null as date_load_UTC  
)

SELECT * FROM renamed_casted
UNION ALL
SELECT * FROM new_row