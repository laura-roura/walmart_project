{% macro macros_copy_stores_csv(table_nm) %} 

DELETE FROM {{var ('raw_db') }}.{{var ('wrk_schema')}}.{{ table_nm }};
 
COPY INTO {{var ('raw_db') }}.{{var ('wrk_schema')}}.{{ table_nm }} 
FROM 
(
SELECT
    $1 AS Store_Id,
    $2 AS Store_Type,
    $3 AS Store_Size,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM @{{ var('stage_name') }}
)
PATTERN = '.*stores.*\.csv'
FILE_FORMAT = {{var ('file_format_csv') }}
PURGE = {{ var('purge_status') }};
 
{% endmacro %}
