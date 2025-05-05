{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['Store_Id','Dept_Id'],
        merge_exclude_columns = ['INSERT_DTS']
    )
}}

with temp_store_dim as
(

    select 
        Store_Id,
        Dept_Id,
        Store_Type,
        Store_Size,
        CREATED_AT,
        CURRENT_TIMESTAMP as INSERT_DTS,
        CURRENT_TIMESTAMP as UPDATE_DTS
    from {{ ref('temp_store_dim') }}

     {% if is_incremental() %}
    where CREATED_AT > (select max(INSERT_DTS) from {{ this }} )
    {% endif %}          
)

select * from temp_store_dim