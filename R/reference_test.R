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
#' @param expr an expression involving at least one \code{dbi.table}.
#'
#' @param envir an environment. Where to evaluate \code{expr}.
#'
#' @param ignore.row.order a logical value. This argument is passed to
#'                         \code{\link{all.equal}}.
#'
#' @param verbose a logical value. When \code{TRUE}, the output from
#'                \code{all.equal} is displaued in a message when
#'                \code{all.equal} returns anything other than \code{TRUE}.
#'
#' @export
reference_test <- function(expr, envir = parent.frame(),
                           ignore.row.order = TRUE, verbose = TRUE) {
  expr <- substitute(expr)

  dbits <- sapply(all.vars(expr), get0, envir = envir, simplify = FALSE)
  dbits  <- dbits[vapply(dbits, is.dbi.table, FALSE)]

  if (!length(dbits)) {
    stop("'expr' must contain at least one dbi.table")
  }

  dbits <- lapply(dbits, as.data.table)

  dbit_eval <- as.data.table(eval(expr, envir = envir))
  dt_eval <- eval(expr, envir = dbits, enclos = envir)

  # merge sets key by default so unkey
  setkey(dbit_eval, NULL)
  setkey(dt_eval, NULL)

  eq <- all.equal(dt_eval, dbit_eval,
                  ignore.row.order = ignore.row.order)

  if (verbose && !isTRUE(eq)) {
    message(eq)
  }

  isTRUE(eq)
}
