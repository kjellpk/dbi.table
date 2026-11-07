SELECT CAST(id AS TEXT) AS foreign_key_name,
       "from" AS column_name,
       "table" AS ref_table_name,
       "to" AS ref_column_name

  FROM pragma_foreign_key_list(?, ?)

 ORDER BY id, seq;
