{{
    config
    (
        materialized='table'
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
)

select * from stores_transformed