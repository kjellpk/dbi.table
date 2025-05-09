WITH confkeys AS (
  SELECT oid, u.confkey, u.idx

    FROM pg_constraint, unnest(confkey) 
    WITH ORDINALITY AS u (confkey, idx)
),

conkeys AS (
  SELECT oid, unnest(conkey) AS conkey

    FROM pg_constraint
)

SELECT CONCAT(pg_constraint.conname, '_', pg_constraint.oid) AS constraint_name,
       current_database() AS fk_table_catalog,
       fkn.nspname AS fk_table_schema,
       fk.relname AS fk_table_name,
       fkc.attname AS fk_column_name,
       current_database() AS pk_table_catalog,
       pkn.nspname AS pk_table_schema,
       pk.relname AS pk_table_name,
       pkc.attname AS pk_column_name,
       CAST (idx AS INTEGER) AS key_ordinal_position

  FROM pg_constraint

 INNER JOIN conkeys
    ON conkeys.oid = pg_constraint.oid

 INNER JOIN pg_class AS fk
    ON fk.oid = pg_constraint.conrelid

 INNER JOIN pg_namespace AS fkn
    ON fkn.oid = fk.relnamespace

 INNER JOIN pg_attribute AS fkc
    ON fkc.attrelid = fk.oid
   AND fkc.attnum = conkeys.conkey

 INNER JOIN pg_class AS pk
    ON  pk.oid = pg_constraint.confrelid

 INNER JOIN pg_namespace AS pkn
    ON pkn.oid = pk.relnamespace

 INNER JOIN confkeys
    ON  confkeys.oid = pg_constraint.oid

 INNER JOIN pg_attribute AS pkc
    ON pkc.attrelid = pg_constraint.confrelid
   AND pkc.attnum = confkeys.confkey

 WHERE pg_constraint.contype = 'f';
