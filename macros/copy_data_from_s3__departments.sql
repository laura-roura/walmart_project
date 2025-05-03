{% macro macros_copy_departments_csv(table_nm) %} 

DELETE FROM {{var ('raw_db') }}.{{var ('wrk_schema')}}.{{ table_nm }};

COPY INTO {{var ('raw_db') }}.{{var ('wrk_schema')}}.{{ table_nm }} 
FROM 
(
SELECT
    $1 AS Store_Id,
    $2 AS Dept_Id,
    $3 AS Department_Date,
    $4 AS Weekly_Sales,
    $5 AS IsHoliday,
    CURRENT_TIMESTAMP() as CREATED_AT
FROM @{{ var('stage_name') }}department.csv
)
FILE_FORMAT = {{var ('file_format_csv') }}
PURGE={{ var('purge_status') }};

{% endmacro %}