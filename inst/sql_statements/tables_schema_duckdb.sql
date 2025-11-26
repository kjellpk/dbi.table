WITH primary_keys AS (

  SELECT key_column_usage.table_catalog AS pk_table_catalog,
         key_column_usage.table_schema AS pk_table_schema,
         key_column_usage.table_name AS pk_table_name,
         key_column_usage.column_name AS pk_column_name,
         key_column_usage.ordinal_position AS pk_ordinal_position

    FROM information_schema.key_column_usage AS key_column_usage

   INNER JOIN information_schema.table_constraints AS table_constraints
      ON (table_constraints.constraint_name = key_column_usage.constraint_name)
     AND (table_constraints.table_catalog = key_column_usage.table_catalog)
     AND (table_constraints.table_schema = key_column_usage.table_schema)
     AND (table_constraints.table_name = key_column_usage.table_name)

   WHERE table_constraints.constraint_type = 'PRIMARY KEY'
)

SELECT "columns".table_catalog AS table_catalog,
       "columns".table_schema AS table_schema,
       "columns".table_name AS table_name,
       "columns".column_name AS column_name,
       "columns".ordinal_position AS ordinal_position,
       primary_keys.pk_ordinal_position AS pk_ordinal_position

  FROM information_schema.columns AS "columns"

  LEFT OUTER JOIN primary_keys
    ON (pk_table_catalog = "columns".table_catalog)
   AND (pk_table_schema = "columns".table_schema)
   AND (pk_table_name = "columns".table_name)
   AND (pk_column_name = "columns".column_name);
 