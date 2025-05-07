WITH indpk AS (
  SELECT indrelid,
         UNNEST(indkey) AS indkey,
         generate_series(1, array_length(indkey, 1)) as pk_ordinal_position

  FROM pg_index

 WHERE indisprimary = TRUE
)

SELECT current_database() AS table_catalog,
       pg_namespace.nspname AS table_schema,
       pg_class.relname AS table_name,
       pg_attribute.attname AS column_name,
       pg_attribute.attnum AS ordinal_position,
       indpk.pk_ordinal_position AS pk_ordinal_position

  FROM pg_class
  
 INNER JOIN pg_namespace
    ON pg_namespace.oid = pg_class.relnamespace

 INNER JOIN pg_attribute
    ON pg_attribute.attrelid = pg_class.oid

  LEFT JOIN indpk
    ON indpk.indrelid = pg_class.oid
   AND indpk.indkey = pg_attribute.attnum

 WHERE pg_attribute.attnum > 0 AND pg_class.relkind IN ('r', 'v');
