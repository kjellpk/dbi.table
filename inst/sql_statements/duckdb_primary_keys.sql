SELECT database_name AS catalog,
       schema_name AS schema,
       table_name AS table,
       UNNEST(constraint_column_names) AS column,
       UNNEST(constraint_column_indexes) + 1 AS column_index

  FROM duckdb_constraints()

 WHERE constraint_type = 'PRIMARY KEY';
