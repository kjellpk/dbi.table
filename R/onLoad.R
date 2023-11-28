#' @import data.table


.onLoad <- function(libname, pkgname) {
  add_special(".N", call("n"), map = special_symbols)
  add_special(".SD", map = special_symbols)
  add_special(".BY", map = special_symbols)
  add_special(".I", map = special_symbols)
  add_special(".GRP", map = special_symbols)
  add_special(".NGRP", map = special_symbols)
  add_special(".EACHI", map = special_symbols)

  add_special(".", as.name("list"), map = special_functions)
  add_special("forder", as.name("order"), map = special_functions)

  NULL
}
