#' @rawNamespace S3method(tables_schema_,PqConnection,tables_schema_postgres)
tables_schema_postgres <- function(catalog) {
  DBI::dbGetQuery(catalog, sql_statement("tables_schema_postgres"))
}



#' @rawNamespace S3method(default_schema,PqConnection,default_schema_postgres)
default_schema_postgres <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT CURRENT_SCHEMA()")[[1L]]
}



#' @rawNamespace S3method(schemas_to_include,PqConnection,schemas_to_include_postgres)
schemas_to_include_postgres <- function(conn) {
  c("information_schema", "pg_catalog")
}



#' @rawNamespace S3method(foreign_keys_,PqConnection,foreign_keys_postgres)
foreign_keys_postgres <- function(catalog, id) {
  DBI::dbGetQuery(catalog, sql_statement("foreign_keys_postgres"))
}
