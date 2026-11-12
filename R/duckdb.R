#' @rawNamespace S3method(catalog_objects,duckdb_connection,catalog_objects_duckdb)
catalog_objects_duckdb <- function(catalog, ...) {
  DBI::dbGetQuery(catalog, sql_statement("duckdb_columns"))
}



#' @rawNamespace S3method(catalog_primary_keys,duckdb_connection,catalog_primary_keys_duckdb)
catalog_primary_keys_duckdb <- function(catalog, ...) {
  DBI::dbGetQuery(catalog, sql_statement("duckdb_primary_keys"))
}



#' @rawNamespace S3method(catalog_foreign_keys,duckdb_connection,catalog_foreign_keys_duckdb)
catalog_foreign_keys_duckdb <- function(catalog, ...) {
  DBI::dbGetQuery(catalog, sql_statement("duckdb_foreign_keys"))
}



#' @rawNamespace S3method(default_schema,duckdb_connection,default_schema_duckdb)
default_schema_duckdb <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT current_schema()")[[1L]]
}



#' @rawNamespace S3method(schemas_to_include,duckdb_connection,schemas_to_include_duckdb)
schemas_to_include_duckdb <- function(conn) {
  c("information_schema")
}



#' @rawNamespace S3method(temporary_dbi_table,duckdb_connection,temporary_dbi_table_duckdb)
temporary_dbi_table_duckdb <- function(conn, x, key = NULL) {
  temp_name <- unique_table_name(TMP_BASE)
  duckdb::duckdb_register(conn, temp_name, x)

  temp_id <- DBI::Id(temp_name)
  x <- new_dbi_table(conn, temp_id, data.frame(field = names(x)), key)

  temp_dbi_table <- new.env(parent = emptyenv())
  temp_dbi_table$name <- temp_name
  temp_dbi_table$conn <- conn
  reg.finalizer(temp_dbi_table, finalize_duckdb_temp_dbi_table)

  attr(x, "temp_dbi_table") <- temp_dbi_table
  x
}



finalize_duckdb_temp_dbi_table <- function(e) {
  stry(duckdb::duckdb_unregister(e$conn, e$name))
  NULL
}
