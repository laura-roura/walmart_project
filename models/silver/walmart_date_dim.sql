{{
    config
    (
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='Date_Id',
        merge_exclude_columns=['INSERT_DTS']
    )
}}

with temp_date_dim as
(
    select
        Date_Id,
        Store_Date,
        IsHoliday,
        CREATED_AT,
        CURRENT_TIMESTAMP as INSERT_DTS,
        CURRENT_TIMESTAMP as UPDATE_DTS
    from {{ ref('temp_date_dim') }}

     {% if is_incremental() %}
    where CREATED_AT > (select max(INSERT_DTS) from {{ this }} )
    {% endif %}          
)

select * from temp_date_dim