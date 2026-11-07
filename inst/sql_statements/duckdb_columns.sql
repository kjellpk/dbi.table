SELECT database_name AS catalog,
       schema_name AS schema,
       table_name AS table,
       column_name AS field,
       column_index AS field_index,
       data_type AS type

  FROM duckdb_columns()

 WHERE database_name <> 'system'

 UNION ALL

SELECT database_name AS catalog,
       schema_name AS schema,
       table_name AS table,
       column_name AS field,
       column_index AS field_index,
       data_type AS type

  FROM duckdb_columns()

 WHERE database_name = 'system'
   AND schema_name IN ('information_schema', 'pg_catalog');
