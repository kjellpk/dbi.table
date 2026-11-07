SELECT schema AS schema,
       name AS "table"

  FROM pragma_table_list 

 WHERE type IN ('table', 'view') 
   AND name NOT LIKE 'sqlite_%';
