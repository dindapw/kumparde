select count(1) as row_count
     , TO_CHAR(MIN($date_column), 'YYYY-MM-DD') min_date
    , TO_CHAR(MAX($date_column), 'YYYY-MM-DD') max_date
FROM $schema_name.$table_name final
WHERE 1=1
$date_condition