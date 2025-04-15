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



#' @rawNamespace S3method(tables_schema,SQLiteConnection,tables_schema_sqlite)
tables_schema_sqlite <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_sqlite"))
}



#' @rawNamespace S3method(tables_schema,MariaDBConnection,tables_schema_mariadb)
tables_schema_mariadb <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_mariadb"))
}



#' @rawNamespace S3method(tables_schema,PqConnection,tables_schema_postgres)
tables_schema_postgres <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_postgres"))
}



#' @rawNamespace S3method(tables_schema,"Microsoft SQL Server",tables_schema_Microsoft_SQL_Server)
tables_schema_Microsoft_SQL_Server <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_Microsoft_SQL_Server"))
}
