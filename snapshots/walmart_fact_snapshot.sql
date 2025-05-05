{% snapshot walmart_fact_snapshot %}

{{
    config
    (
        strategy = 'check',
        unique_key = ['DATE_ID', 'STORE_ID', 'DEPT_ID'],
        check_cols = ['Weekly_Sales','Fuel_Price','Temperature','Unemployment', 'CPI', 'Markdown1', 'Markdown2', 'Markdown3', 'Markdown4', 'Markdown5'],
        schema = 'SILVER'
    )
}}


select * from {{ source('facts','TEMP_FACT_TABLE') }}

{% endsnapshot %}