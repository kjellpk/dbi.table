#' @import data.table

session <- new.env(parent = emptyenv())
session$table_name_counter <- 0L
session$print <- character(1L)
session$special_symbols <- new.env(parent = emptyenv())
session$key_base <- "._.xcwrl_"
session$tmp_base <- "DBI_TABLE_PACKAGE_TEMPORARY_TABLE_"

.onLoad <- function(libname, pkgname) {
  add_special(".N", function(e, dbi_table, specials, env) call("n"))
  add_special(".SD")
  add_special(".BY")
  add_special(".I")
  add_special(".GRP")
  add_special(".NGRP")
  add_special(".EACHI")

  #add_special(".", special_list)
  #add_special("list", special_list)
  add_special("%in%", special_in)
  add_special("%chin%", special_in)
  add_special("local", special_local)
  add_special("!", special_not)
  add_special("%like%", special_like)
  add_special("%LIKE%", special_LIKE)

  NULL
}
