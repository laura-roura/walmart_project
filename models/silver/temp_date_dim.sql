{{
    config
    (
        materialized='table'
    )
}}

with 

distinct_date_src as
(
    select 
        DISTINCT Department_Date,
        IsHoliday,
        CREATED_AT       
    from {{ ref('departments') }}
    order by 1
),

departments_src as
(
    select 
        {{ dbt_utils.generate_surrogate_key(['Department_Date']) }} as Date_Id,
        Department_Date as Store_Date,
        IsHoliday,
        CREATED_AT       
    from distinct_date_src
)

select * from departments_src