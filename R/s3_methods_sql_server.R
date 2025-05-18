#' @rawNamespace S3method(tables_schema_,"Microsoft SQL Server",tables_schema_Microsoft_SQL_Server)
tables_schema_Microsoft_SQL_Server <- function(catalog) {
  DBI::dbGetQuery(catalog, sql_statement("tables_schema_Microsoft_SQL_Server"))
}



#' @rawNamespace S3method(default_schema,"Microsoft SQL Server",default_schema_Microsoft_SQL_Server)
default_schema_Microsoft_SQL_Server <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT SCHEMA_NAME()")[[1L]]
}



#' @rawNamespace S3method(schemas_to_include,"Microsoft SQL Server",schemas_to_include_Microsoft_SQL_Server)
schemas_to_include_Microsoft_SQL_Server <- function(conn) {
  c("INFORMATION_SCHEMA", "sys")
}



#' @rawNamespace S3method(foreign_keys_,"Microsoft SQL Server",foreign_keys_Microsoft_SQL_Server)
foreign_keys_Microsoft_SQL_Server <- function(catalog, id) {
  r <- merge(catalog$INFORMATION_SCHEMA$REFERENTIAL_CONSTRAINTS,
             catalog$INFORMATION_SCHEMA$KEY_COLUMN_USAGE,
             by = c("CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME"))

  names(r) <- paste0("fk_", names(r))

  r <- merge(catalog$INFORMATION_SCHEMA$KEY_COLUMN_USAGE, r,
             by.x = c("CONSTRAINT_CATALOG",
                      "CONSTRAINT_SCHEMA",
                      "CONSTRAINT_NAME",
                      "ORDINAL_POSITION"),
             by.y = c("fk_UNIQUE_CONSTRAINT_CATALOG",
                      "fk_UNIQUE_CONSTRAINT_SCHEMA",
                      "fk_UNIQUE_CONSTRAINT_NAME",
                      "fk_ORDINAL_POSITION"))
  
  r <- r[, c("fk_CONSTRAINT_NAME", "fk_TABLE_CATALOG", "fk_TABLE_SCHEMA",
              "fk_TABLE_NAME", "fk_COLUMN_NAME", "TABLE_CATALOG",
              "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
              "ORDINAL_POSITION")]

  names(r) <- c("constraint_name", "fk_table_catalog", "fk_table_schema",
                "fk_table_name", "fk_column_name", "pk_table_catalog",
                "pk_table_schema", "pk_table_name", "pk_column_name",
                "key_ordinal_position")

  as.data.frame(r, n = -1L)
}
