{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['Store_Id','Dept_Id'],
        merge_exclude_columns = ['INSERT_DTS']
    )
}}

with 
stores_src as
(
    select 
        Store_Id,
        Store_Type,
        Store_Size,
        CREATED_AT    
    from {{ ref('stores') }}
),

departments_src as
(
    select
        DISTINCT Store_Id, Dept_Id
    from {{ ref('departments') }}
    order by 1,2
),

stores_transformed as
(
    select 
        a.Store_Id,
        b.Dept_Id,
        a.Store_Type,
        a.Store_Size,
        a.CREATED_AT
        from stores_src a join departments_src b on (a.Store_Id = b.Store_Id)        
),

temp_store_dim as
(
    select 
        Store_Id,
        Dept_Id,
        Store_Type,
        Store_Size,
        CREATED_AT,
        CURRENT_TIMESTAMP as INSERT_DTS,
        CURRENT_TIMESTAMP as UPDATE_DTS
    from stores_transformed

     {% if is_incremental() %}
    where CREATED_AT > (select max(INSERT_DTS) from {{ this }} )
    {% endif %}          
)

select * from temp_store_dim