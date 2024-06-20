#' @import data.table

session <- new.env(parent = emptyenv())
session$table_name_counter <- 0L
session$print <- ""
session$special_symbols <- new.env(parent = emptyenv())
session$key_base <- "._.xcwrl_"
session$tmp_base <- "DBI_TABLE_PACKAGE_TEMPORARY_TABLE_"
session$max_fetch <- 10000L
session$max_in_query <- 1000L

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



  dbQuoteIdentifier_MariaDBConnection_Id <- function(conn, x, ...) {
    if (length(name <- x@name) == 3L && name[[1L]] == "def") {
      name <- name[-1L]
    }

    DBI::SQL(paste0(DBI::dbQuoteIdentifier(conn, name), collapse = "."))
  }

  meth <- call("setMethod",
               f = "dbQuoteIdentifier",
               signature = c("MariaDBConnection", "Id"),
               definition = dbQuoteIdentifier_MariaDBConnection_Id)

  if (isNamespaceLoaded("RMariaDB")) {
    eval(meth, envir = .GlobalEnv)
  } else {
    setHook(packageEvent("RMariaDB", "onLoad"),
      function(...) {
        eval(meth, envir = .GlobalEnv)
      }
    )
  }

  NULL
}
