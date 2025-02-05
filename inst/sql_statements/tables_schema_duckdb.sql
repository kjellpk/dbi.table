SELECT "columns".table_catalog AS table_catalog,
       "columns".table_schema AS table_schema,
       "columns".table_name AS table_name,
       "columns".column_name AS column_name,
       "columns".ordinal_position AS ordinal_position,
       key_column_usage.ordinal_position AS pk_ordinal_position

  FROM information_schema.columns AS "columns"

  LEFT OUTER JOIN information_schema.table_constraints AS table_constraints
    ON ("columns".table_catalog = table_constraints.table_catalog)
   AND ("columns".table_schema = table_constraints.table_schema)
   AND ("columns".table_name = table_constraints.table_name)

  LEFT OUTER JOIN information_schema.key_column_usage AS key_column_usage
    ON (table_constraints.constraint_name = key_column_usage.constraint_name)
   AND ("columns".table_catalog = key_column_usage.table_catalog)
   AND ("columns".table_schema = key_column_usage.table_schema)
   AND ("columns".table_name = key_column_usage.table_name)
   AND ("columns".column_name = key_column_usage.column_name)

 WHERE table_constraints.constraint_type = 'PRIMARY KEY'
 