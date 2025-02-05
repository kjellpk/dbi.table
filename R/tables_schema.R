tables_schema <- function(conn) {
  UseMethod("tables_schema")
}



#' @rawNamespace S3method(tables_schema, default, tables_schema_default)
tables_schema_default <- function(conn) {
  columns <- try(DBI::dbGetQuery(conn, sql_statement("tables_schema_default")),
                 silent = TRUE)

  if (is.data.frame(columns)) {
    names(columns) <- tolower(names(columns))
    columns$pk_ordinal_position <- NA_integer_
    return(columns)
  }

  NULL
}


#' @rawNamespace S3method(tables_schema, duckdb_connection, tables_schema_duckdb)
tables_schema_duckdb <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_duckdb"))
}
