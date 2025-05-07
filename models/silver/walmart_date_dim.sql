{{
    config
    (
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='Date_Id',
        merge_exclude_columns=['INSERT_DTS']
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

temp_date_dim as
(
    select 
        {{ dbt_utils.generate_surrogate_key(['Department_Date']) }} as Date_Id,
        Department_Date as Store_Date,
        IsHoliday,
        CREATED_AT,
        CURRENT_TIMESTAMP as INSERT_DTS,
        CURRENT_TIMESTAMP as UPDATE_DTS         
    from distinct_date_src

    {% if is_incremental() %}
    where CREATED_AT > (select max(INSERT_DTS) from {{ this }} )
    {% endif %}      
)

select * from temp_date_dim