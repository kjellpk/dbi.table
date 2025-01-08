session <- new.env(parent = emptyenv())
session$table_name_counter <- 0L
session$print <- NULL
session$special_symbols <- new.env(parent = emptyenv())
session$key_base <- "._.xcwrl_"
session$tmp_base <- "DBI_TABLE_PACKAGE_TEMPORARY_TABLE_"
session$ignore_schemas <- c("information_schema", "pg_catalog")
session$default_information_schema_tables <- c("columns",
                                               "key_column_usage",
                                               "referential_constraints",
                                               "tables")

.onLoad <- function(libname, pkgname) {
  add_special(".N", function(e, dbi_table, specials, env) call("n"))
  add_special(".SD")
  add_special(".BY")
  add_special(".I")
  add_special(".GRP")
  add_special(".NGRP")
  add_special(".EACHI")

  add_special("%in%", special_in)
  add_special("%chin%", special_in)
  add_special("local", special_local)
  add_special("!", special_not)
  add_special("%like%", special_like)
  add_special("%LIKE%", special_LIKE)

  # Case suggests data.table:
  # if (isNamespaceLoaded("data.table")) {
  #   .S3method("as.data.table", "dbi.table", as_data_table)
  # } else {
  #   setHook(packageEvent("data.table", "onLoad"), function(pkgname, pkgpath) {
  #     cl <- call(".S3method",
  #                generic = "as.data.table",
  #                class = "dbi.table",
  #                method = as_data_table)
  #     eval(cl, envir = asNamespace(pkgname))
  #   })
  # }

  NULL
}
