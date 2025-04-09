tables_schema <- function(conn) {
  UseMethod("tables_schema")
}



#' @rawNamespace S3method(tables_schema,default,tables_schema_default)
tables_schema_default <- function(conn) {
  columns <- try(DBI::dbGetQuery(conn, sql_statement("tables_schema_default")),
                 silent = TRUE)

  if (is.data.frame(columns)) {
    names(columns) <- tolower(names(columns))
  } else {
    columns <- mapply(function(conn, name) {
      fields <- DBI::dbListFields(conn, name)
      data.frame(table_name = name,
                 column_name = fields,
                 ordinal_position = seq_along(fields))
    },
    name = DBI::dbListTables(conn),
    MoreArgs = list(conn = conn), SIMPLIFY = FALSE)

    columns <- do.call(rbind, columns)
    row.names(columns) <- NULL
  }

  columns$pk_ordinal_position <- NA_integer_
  columns
}



#' @rawNamespace S3method(tables_schema,duckdb_connection,tables_schema_duckdb)
tables_schema_duckdb <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_duckdb"))
}



#' @rawNamespace S3method(tables_schema,SQLiteConnection,tables_schema_sqlite)
tables_schema_sqlite <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_sqlite"))
}



#' @rawNamespace S3method(tables_schema,MariaDBConnection,tables_schema_mariadb)
tables_schema_mariadb <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_mariadb"))
}
