#' @rawNamespace S3method(default_schema,MariaDBConnection,default_schema_mariadb)
default_schema_mariadb <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT SCHEMA()")[[1L]]
}



#' @rawNamespace S3method(tables_schema,MariaDBConnection,tables_schema_mariadb)
tables_schema_mariadb <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_mariadb"))
}



#' @rawNamespace S3method(schemas_to_include,MariaDBConnection,schemas_to_include_mariadb)
schemas_to_include_mariadb <- function(conn) {
  c("information_schema")
}
