#' @import data.table

session <- new.env(parent = emptyenv())
session$table_name_counter <- 0L
session$print <- ""
session$special_symbols <- new.env(parent = emptyenv())



.onLoad <- function(libname, pkgname) {
  add_special(".N", call("n"))
  add_special(".SD")
  add_special(".BY")
  add_special(".I")
  add_special(".GRP")
  add_special(".NGRP")
  add_special(".EACHI")

  add_special(".", as.name("list"))
  add_special("forder", as.name("order"))

  NULL
}
