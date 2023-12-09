#' @import data.table

session <- new.env(parent = emptyenv())
session$table_name_counter <- 0L
session$"name" <- new.env(parent = emptyenv())
session$special_functions <- new.env(parent = emptyenv())



.onLoad <- function(libname, pkgname) {
  add_special(".N", call("n"), map = "name")
  add_special(".SD", map = "name")
  add_special(".BY", map = "name")
  add_special(".I", map = "name")
  add_special(".GRP", map = "name")
  add_special(".NGRP", map = "name")
  add_special(".EACHI", map = "name")

  add_special(".", as.name("list"), map = "call")
  add_special("forder", as.name("order"), map = "call")

  NULL
}
