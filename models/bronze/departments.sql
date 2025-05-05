{{
    config
    (
        pre_hook = macros_copy_departments_csv('DEPARTMENTS_SOURCE'),
        materialized = 'incremental',
        incremental_strategy = 'append'
    )
}}

with department_src as 
(
    select
        Store_Id,
        Dept_Id,
        Department_Date,
        Weekly_Sales,
        IsHoliday,
        CREATED_AT,
        CURRENT_TIMESTAMP() as INSERT_DTS
    from {{ source('departments', 'DEPARTMENTS_SOURCE') }}

    {% if is_incremental() %}
    where CREATED_AT > (SELECT MAX(INSERT_DTS) FROM {{ this }})
    {% endif %}
)

select * from department_src