WITH "pk" AS (
  SELECT "object_id" ,"index_id"
    FROM "sys"."indexes"
   WHERE "is_primary_key" = 1
)

SELECT DB_NAME() AS "table_catalog",
       "s"."name" AS "table_schema",
       "ao"."name" AS "table_name",
	     "ac"."name" AS "column_name",
	     "ac"."column_id" AS "ordinal_position",
	     "ic"."key_ordinal" AS "pk_ordinal_position"

  FROM "sys"."all_objects" AS "ao"

 INNER JOIN "sys"."schemas" AS "s"
    ON "s"."schema_id" = "ao"."schema_id"

 INNER JOIN "sys"."all_columns" AS "ac"
    ON "ac"."object_id" = "ao"."object_id"

  LEFT JOIN "pk"
    ON "pk"."object_id" = "ao"."object_id"

  LEFT JOIN "sys"."index_columns" AS "ic"
    ON "ic"."object_id" = "pk"."object_id"
   AND "ic"."index_id" = "pk"."index_id"
   AND "ic"."column_id" = "ac"."column_id"

 WHERE "ao"."type" IN ('U', 'V')

 ORDER BY "table_schema", "table_name", "ordinal_position";
