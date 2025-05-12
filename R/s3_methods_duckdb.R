#' @rawNamespace S3method(tables_schema_,duckdb_connection,tables_schema_duckdb)
tables_schema_duckdb <- function(conn) {
  schema <- DBI::dbGetQuery(conn, sql_statement("tables_schema_duckdb"))

  information_schema_tables <- c("character_sets", "columns",
                                 "constraint_column_usage", "key_column_usage",
                                 "referential_constraints", "schemata",
                                 "tables", "table_constraints")

  for (table in information_schema_tables) {
    i <- DBI::dbListFields(conn, DBI::SQL(paste0("information_schema.", table)))
    i <- data.frame(table_catalog = "system",
                    table_schema = "information_schema",
                    table_name = table,
                    column_name = i,
                    ordinal_position = seq_along(i),
                    pk_ordinal_position = NA_integer_)
    schema <- rbind(schema, i)
  }

  schema
}



#' @rawNamespace S3method(default_schema,duckdb_connection,default_schema_duckdb)
default_schema_duckdb <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT current_schema()")[[1L]]
}



#' @rawNamespace S3method(schemas_to_include,duckdb_connection,schemas_to_include_duckdb)
schemas_to_include_duckdb <- function(conn) {
  c("information_schema")
}



#' @rawNamespace S3method(foreign_keys_,duckdb_connection,foreign_keys_duckdb)
foreign_keys_duckdb <- function(catalog, id) {
  r <- merge(catalog$information_schema$referential_constraints,
             catalog$information_schema$key_column_usage,
             by = c("constraint_catalog",
                    "constraint_schema",
                    "constraint_name"))

  names(r) <- paste0("fk_", names(r))

  r <- merge(catalog$information_schema$key_column_usage, r,
             by.x = c("constraint_catalog",
                      "constraint_schema",
                      "constraint_name",
                      "ordinal_position"),
             by.y = c("fk_unique_constraint_catalog",
                      "fk_unique_constraint_schema",
                      "fk_unique_constraint_name",
                      "fk_ordinal_position"))

  r <- r[, c("fk_constraint_name", "fk_table_catalog", "fk_table_schema",
              "fk_table_name", "fk_column_name", "table_catalog",
              "table_schema", "table_name", "column_name",
              "fk_position_in_unique_constraint")]

  names(r) <- c("constraint_name", "fk_table_catalog", "fk_table_schema",
                "fk_table_name", "fk_column_name", "pk_table_catalog",
                "pk_table_schema", "pk_table_name", "pk_column_name",
                "key_ordinal_position")

  as.data.frame(r, n = -1L)
}
