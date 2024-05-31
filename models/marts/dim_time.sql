WITH dates AS (
    SELECT
        DATEADD(day, SEQ4(), '2024-01-01') AS date
    FROM
        TABLE(GENERATOR(ROWCOUNT => 50)) 
),

final AS (
select
    {{dbt_utils.generate_surrogate_key(['date'])}} as time_key,
    date as time_id,
    extract(year from date) as year,
    extract(month from date) as month,
    monthname(date) as month_name,
    extract(day from date) as day,
    extract(dayofweek from date) as number_week_day,
    dayname(date) as week_day,
    extract(quarter from date) as quarter
from dates
)

SELECT * FROM final