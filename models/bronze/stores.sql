{{
    config
    (
        pre_hook = macros_copy_stores_csv('STORES_SOURCE'),
        materialized = 'incremental',
        incremental_strategy = 'append'
    )
}}

with store_src as 
(
    select
        Store_Id,
        Store_Type,
        Store_Size,
        CREATED_AT,
        CURRENT_TIMESTAMP() as INSERT_DTS
    from {{ source('stores', 'STORES_SOURCE') }}

    {% if is_incremental() %}
    where CREATED_AT > (SELECT MAX(INSERT_DTS) FROM {{ this }})
    {% endif %}
)

select * from store_src