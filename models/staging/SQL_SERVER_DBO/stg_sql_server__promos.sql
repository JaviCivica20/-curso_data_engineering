WITH src_promos AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
          md5(promo_id) as promo_id
        , promo_id AS promo_name
        , discount AS discount_dollars
        , status
        , _fivetran_deleted AS date_deleted
        , _fivetran_synced AS date_load_UTC
    FROM src_promos
    ),

new_row as (
    select
        md5('Sin promocion') as promo_id,
        'Sin promocion' as promo_name,
        0 as discount_dollars,  
        'active' as satus,  
        null as date_deleted, 
        null as date_load_UTC  
)

SELECT * FROM renamed_casted
UNION
SELECT * FROM new_row