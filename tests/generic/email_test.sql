SELECT email 
FROM {{ ref('stg_sql_server__users') }}
WHERE email LIKE '%@%.%'