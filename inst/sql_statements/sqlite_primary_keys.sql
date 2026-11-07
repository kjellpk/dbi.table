SELECT name AS column

  FROM pragma_table_info(?, ?)

 WHERE pk > 0
 
 ORDER BY pk;
