#' \code{dbi.table} test vs. reference implementation
#'
#' Evaluate an expression using \code{dbi.table}s and separately using
#' \code{data.tables}s, then compare the results.
#'
#' This function evaluates the input \code{x} in two different ways:
#'
#'   1. (the typical dbi.table workflow) the input is translated to sql,
#'      evaluated by the connected database, and the results are downloaded as
#'      a data.table.
#'
#'   2. (reference) the entire database table is downloaded to as data.table
#'      then the expression is evaluated locally.
#'
#' The goal of the dbi.table package is to match the reference result. Matching
#' up to row order is acceptable.
#'
#' @param x an expression involving \code{dbi.table}s.
#'
#' @param envir an environment. Where to evaluate \code{x}.
#'
#' @param ignore.row.order a logical value. This argument is passed to
#'                         \code{\link{all.equal}}.
#'
#' @param verbose a logical value. When \code{TRUE}, a summary of the comparison
#'                is printed in the terminal.
#'
#' @export
reference_test <- function(x, envir = parent.frame(), ignore.row.order = TRUE,
                           verbose = TRUE) {
  x <- substitute(x)

  if (is.dbi.table(dbi_table_eval <- eval(x, envir = envir))) {
    dbi_table_eval <- as.data.table(dbi_table_eval)
  }

  check_env <- new.env(parent = envir)

  for(v in all.vars(x)) {
    tmp <- try(eval(as.name(v), envir = envir), silent = TRUE)
    if (is.dbi.table(tmp)) {
      assign(v, as.data.table(tmp), pos = check_env)
    }
  }

  data_table_eval <- eval(x, envir = check_env)

  # merge sets key by default so unkey
  setkey(data_table_eval, NULL)

  eq <- all.equal(dbi_table_eval,
                  data_table_eval,
                  ignore.row.order = ignore.row.order)

  if (verbose) {
    if (!is.data.table(dbi_table_eval)) {
      cat("\n  [MAJOR] expression", sQuote(format(x)), "did not evaluate to a",
          sQuote("dbi.table"), "\n")
      eq <- FALSE
    }

    if (!length(ls(check_env))) {
      cat("\n  [MAJOR] expression", sQuote(format(x)),
          "does not include at least one", sQuote("dbi.table"), "\n")
      eq <- FALSE
    }

    for (DT in ls(check_env)) {
      cat("\n   [INFO] input", sQuote("dbi.table"), DT, "preview\n\n")
      print(first(check_env[[DT]], 2), row.names = FALSE)
      cat("\n")
    }

    cat("\n [RESULT] evaluated as", sQuote("dbi.table"), "preview\n\n")
    print(first(dbi_table_eval, 5), row.names = FALSE)
    cat("\n")

    cat("\n [RESULT] evaluated as", sQuote("data.table"), "preview\n\n")
    print(first(data_table_eval, 5), row.names = FALSE)
    cat("\n")

    cat("\n   [INFO] all.equal\n\n")
    print(eq)
    cat("\n")
  }

  invisible(isTRUE(eq))
}
