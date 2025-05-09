#' @rawNamespace S3method(tables_schema,MariaDBConnection,tables_schema_mariadb)
tables_schema_mariadb <- function(conn) {
  DBI::dbGetQuery(conn, sql_statement("tables_schema_mariadb"))
}



#' @rawNamespace S3method(default_schema,MariaDBConnection,default_schema_mariadb)
default_schema_mariadb <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT SCHEMA()")[[1L]]
}



#' @rawNamespace S3method(schemas_to_include,MariaDBConnection,schemas_to_include_mariadb)
schemas_to_include_mariadb <- function(conn) {
  c("information_schema")
}



#' @rawNamespace S3method(foreign_keys_,MariaDBConnection,foreign_keys_mariadb)
foreign_keys_mariadb <- function(catalog, id) {
  r <- merge(catalog$information_schema$TABLE_CONSTRAINTS,
             catalog$information_schema$KEY_COLUMN_USAGE,
             by = c("CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "TABLE_SCHEMA",
                    "TABLE_NAME"))

  CONSTRAINT_TYPE <- NULL
  r <- as.data.frame(r[CONSTRAINT_TYPE == "FOREIGN KEY"], n = -1L)

  r$constraint_name <- paste(r$CONSTRAINT_CATALOG,
                             r$CONSTRAINT_SCHEMA,
                             r$CONSTRAINT_NAME,
                             sep = "_")

  r <- r[, c("constraint_name", "CONSTRAINT_CATALOG", "TABLE_SCHEMA",
             "TABLE_NAME", "COLUMN_NAME", "CONSTRAINT_CATALOG",
             "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME",
             "REFERENCED_COLUMN_NAME", "POSITION_IN_UNIQUE_CONSTRAINT")]

  names(r) <- c("constraint_name", "fk_table_catalog", "fk_table_schema",
                "fk_table_name", "fk_column_name", "pk_table_catalog",
                "pk_table_schema", "pk_table_name", "pk_column_name",
                "key_ordinal_position")

  r
}
