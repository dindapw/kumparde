MERGE INTO $target_schema.$target_table
    USING $source_schema.$source_table source_table
    ON $join_column
WHEN MATCHED THEN
    UPDATE SET $matched_columns
WHEN NOT MATCHED THEN
    INSERT ($target_columns) VALUES ($source_columns);