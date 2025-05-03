{{
    config
    (
        pre_hook = macros_copy_facts_csv('FACTS_SOURCE'),
        materialized = 'incremental',
        incremental_strategy = 'append'
    )
}}

with fact_src as 
(
    select
        Store_Id,
        Fact_Date,
        Temperature,
        Fuel_Price,
        Markdown1,
        Markdown2,
        Markdown3,
        Markdown4,
        Markdown5,
        CPI,
        Unemployment,
        IsHoliday,
        CREATED_AT,
        CURRENT_TIMESTAMP() as INSERT_DTS
    from {{ source('facts', 'FACTS_SOURCE') }}

    {% if is_incremental() %}
    where CREATED_AT > (SELECT MAX(INSERT_DTS) FROM {{ this }})
    {% endif %}
)

select * from fact_src