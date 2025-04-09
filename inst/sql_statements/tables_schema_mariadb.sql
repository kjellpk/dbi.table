WITH PK AS (
  SELECT TABLE_CATALOG,
         TABLE_SCHEMA,
         TABLE_NAME,
         COLUMN_NAME,
         ORDINAL_POSITION

    FROM information_schema.key_column_usage

   WHERE CONSTRAINT_NAME = 'PRIMARY'
)

SELECT C.TABLE_CATALOG AS table_catalog,
       C.TABLE_SCHEMA AS table_schema,
       C.TABLE_NAME AS table_name,
       C.COLUMN_NAME AS column_name,
       C.ORDINAL_POSITION AS ordinal_position,
       PK.ORDINAL_POSITION AS pk_ordinal_position

  FROM information_schema.columns AS C

  LEFT JOIN PK
    ON PK.TABLE_CATALOG = C.TABLE_CATALOG
   AND PK.TABLE_SCHEMA = C.TABLE_SCHEMA
   AND PK.TABLE_NAME = C.TABLE_NAME
   AND PK.COLUMN_NAME = C.COLUMN_NAME;
