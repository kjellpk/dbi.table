SELECT tc.constraint_name AS foreign_key_name,
       fk.table_catalog AS table_catalog,
       fk.table_schema AS table_schema,
       fk.table_name AS table_name,
       fk.column_name AS column_name,
       pk.table_catalog AS ref_table_catalog,
       pk.table_schema AS ref_table_schema,
       pk.table_name AS ref_table_name,
       pk.column_name AS ref_column_name

  FROM information_schema.table_constraints AS tc

 INNER JOIN information_schema.referential_constraints AS rc
    ON rc.constraint_catalog = tc.constraint_catalog
   AND rc.constraint_schema = tc.constraint_schema
   AND rc.constraint_name = tc.constraint_name

 INNER JOIN information_schema.key_column_usage AS fk
    ON fk.constraint_catalog = tc.constraint_catalog
   AND fk.constraint_schema = tc.constraint_schema
   AND fk.constraint_name = tc.constraint_name

 INNER JOIN information_schema.key_column_usage AS pk
    ON pk.constraint_catalog = rc.unique_constraint_catalog
   AND pk.constraint_schema = rc.unique_constraint_schema
   AND pk.constraint_name = rc.unique_constraint_name
   AND pk.ordinal_position = fk.position_in_unique_constraint

 WHERE tc.constraint_type = 'FOREIGN KEY'

 ORDER BY fk.table_catalog,
          fk.table_schema,
          fk.table_name,
          fk.constraint_name,
          fk.ordinal_position;
