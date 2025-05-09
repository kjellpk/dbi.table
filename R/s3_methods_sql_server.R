#' @rawNamespace S3method(tables_schema,"Microsoft SQL Server",tables_schema_Microsoft_SQL_Server)
tables_schema_Microsoft_SQL_Server <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_Microsoft_SQL_Server"))
}



#' @rawNamespace S3method(default_schema,"Microsoft SQL Server",default_schema_Microsoft_SQL_Server)
default_schema_Microsoft_SQL_Server <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT SCHEMA_NAME()")[[1L]]
}



#' @rawNamespace S3method(schemas_to_include,"Microsoft SQL Server",schemas_to_include_Microsoft_SQL_Server)
schemas_to_include_Microsoft_SQL_Server <- function(conn) {
  c("INFORMATION_SCHEMA", "sys")
}



#' @rawNamespace S3method(foreign_keys_,"Microsoft SQL Server",foreign_keys_duckdb)
foreign_keys_Microsoft_SQL_Server <- function(catalog, id) {
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
