session <- new.env(parent = emptyenv())
session$table_name_counter <- 0L
session$print <- NULL

session$key_base <- "+xcwrl+"
session$tmp_base <- "DBI_TABLE_PACKAGE_TEMPORARY_TABLE_"
session$ignore_schemas <- c("information_schema", "pg_catalog")
session$default_information_schema_tables <- c("character_sets",
                                               "columns",
                                               "constraint_column_usage",
                                               "key_column_usage",
                                               "referential_constraints",
                                               "schemata",
                                               "tables",
                                               "table_constraints")

.onLoad <- function(libname, pkgname) {


  NULL
}
