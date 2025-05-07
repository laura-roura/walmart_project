{% macro macros_copy_facts_csv(table_nm) %} 

DELETE FROM {{var ('raw_db') }}.{{var ('wrk_schema')}}.{{ table_nm }};
 
COPY INTO {{var ('raw_db') }}.{{var ('wrk_schema')}}.{{ table_nm }} 
FROM 
(
SELECT
    $1 AS Store_Id,
    $2 AS Fact_Date,
    $3 AS Temperature,
    $4 AS Fuel_Price,
    CASE WHEN $5 = 'NA' THEN 0 ELSE $5 END AS Markdown1,
    CASE WHEN $6 = 'NA' THEN 0 ELSE $6 END AS Markdown2,
    CASE WHEN $7 = 'NA' THEN 0 ELSE $7 END AS Markdown3,
    CASE WHEN $8 = 'NA' THEN 0 ELSE $8 END AS Markdown4,
    CASE WHEN $9 = 'NA' THEN 0 ELSE $9 END AS Markdown5,
    CASE WHEN $10 = 'NA' THEN 0 ELSE $10 END AS CPI,
    CASE WHEN $11 = 'NA' THEN 0 ELSE $11 END AS Unemployment,
    $12 AS IsHoliday,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM @{{ var('stage_name') }}
)
PATTERN = '.*fact.*\.csv'
FILE_FORMAT = {{var ('file_format_csv') }}
PURGE={{ var('purge_status') }};
 
{% endmacro %}
