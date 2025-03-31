SELECT table_list.schema AS table_schema,
       table_list.name AS table_name,
       table_info.name AS column_name,
       table_info.cid + 1 AS ordinal_position,
       CASE
         WHEN table_info.pk > 0 THEN table_info.pk
         ELSE NULL
       END AS pk_ordinal_position
  FROM pragma_table_list() AS table_list
 INNER JOIN pragma_table_info(table_list.name, table_list.schema) AS table_info
 WHERE table_list.schema = 'main';
