SELECT `id`, `seq` + 1 AS `seq`, `table`, `from`, `to`
  FROM pragma_foreign_key_list(%s);
