#' @rawNamespace S3method(tables_schema_,SQLiteConnection,tables_schema_sqlite)
tables_schema_sqlite <- function(catalog) {
  DBI::dbGetQuery(catalog, sql_statement("tables_schema_sqlite"))
}



#' @rawNamespace S3method(foreign_keys_,SQLiteConnection,foreign_keys_sqlite)
foreign_keys_sqlite <- function(catalog, id) {
  schema <- id@name[["table_schema"]]
  name <- id@name[["table_name"]]

  statement <- sql_statement("foreign_keys_sqlite")
  statement <- sprintf(statement, DBI::dbQuoteString(catalog, name))

  if (m <- nrow(fkl <- DBI::dbGetQuery(catalog, statement))) {
    data.frame(constraint_name = paste(rep("FK", m), fkl$id, sep = "__"),
               fk_table_schema = rep(schema, m),
               fk_table_name = rep(name, m),
               fk_column_name = fkl$from,
               pk_table_schema = rep(schema, m),
               pk_table_name = fkl$table,
               pk_column_name = fkl$to,
               key_ordinal_position = fkl$seq)
  } else {
    NULL
  }
}
