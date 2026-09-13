SELECT DB_NAME() AS "table_catalog",
       "sc"."name" AS "table_schema",
       "ao"."name" AS "table_name",
       "ac"."name" AS "column_name",
       "tp"."name" AS "column_type",
       "ac"."column_id" AS "ordinal_position",
       "ic"."key_ordinal" AS "pk_ordinal_position"

  FROM "sys"."all_objects" AS "ao"

 INNER JOIN "sys"."schemas" AS "sc"
    ON "sc"."schema_id" = "ao"."schema_id"

 INNER JOIN "sys"."all_columns" AS "ac"
    ON "ac"."object_id" = "ao"."object_id"

 INNER JOIN "sys"."types" AS "tp"
    ON "tp"."system_type_id" = "ac"."system_type_id"
   AND "tp"."user_type_id" = "tp"."system_type_id"

  LEFT JOIN "sys"."indexes" AS "in"
    ON "in"."object_id" = "ao"."object_id"
   AND "in"."is_primary_key" = 1

  LEFT JOIN "sys"."index_columns" AS "ic"
    ON "ic"."object_id" = "in"."object_id"
   AND "ic"."index_id"  = "in"."index_id"
   AND "ic"."column_id" = "ac"."column_id"
   AND "ic"."key_ordinal" > 0

 WHERE "ao"."type" IN ('U', 'V')

 ORDER BY "sc"."name", "ao"."name", "ac"."column_id";
