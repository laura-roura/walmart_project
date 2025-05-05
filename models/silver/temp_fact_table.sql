{{
    config
    (
        materialized='table'
    )
}}

with 

date_dim_src as
(
    select 
        Date_Id,
        Store_Date    
    from {{ ref('walmart_date_dim') }}
),

department_src as
(
    select
        Store_Id,
        Department_Date,
        Dept_Id,
        Weekly_Sales
    from {{ source('department', 'DEPARTMENTS') }}
),

dept_dates_src as
(
    select 
       a.Date_Id,
       a.Store_Date,
       b.Store_Id,
       b.Dept_Id,
       b.Weekly_Sales
    from date_dim_src a join department_src b on (a.Store_Date = b.Department_Date)
),

fact_src as
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
        CREATED_AT  
    from {{ ref('facts') }}
),

facts_transformed as
(
    select 
       a.Date_Id,
       a.Store_Id,
       a.Dept_Id,
       a.Weekly_Sales,
       b.Fuel_Price,
       b.Temperature,
       b.Unemployment,
       b.CPI,
       b.Markdown1,
       b.Markdown2,
       b.Markdown3,
       b.Markdown4,
       b.Markdown5,
       b.CREATED_AT
    from dept_dates_src a join fact_src b on ((a.Store_Id = b.Store_Id) and (a.Store_Date = b.Fact_Date)) 

)

select * from facts_transformed