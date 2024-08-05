select count(1) row_count,
      date_format(MIN($date_column),'%Y-%m-%d') min_date,
      date_format(MAX($date_column),'%Y-%m-%d') max_date
from $schema_name.$table_name
WHERE 1=1
$date_condition