with
    stg_budget as (
        select * 
        from {{ ref("stg_google_sheets__budget") }}),

    fct_budget as (
        select 
        _row, 
        product_id, 
        quantity, 
        month 
        from stg_budget
)

select * from fct_budget
