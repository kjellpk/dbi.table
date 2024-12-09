#' Test \code{dbi.table} vs. Reference Implementation
#'
#' Evaluate an expression including at least one \code{dbi.table} and compare
#' the result with the \emph{Reference Implementation}. This function is
#' primarily for testing and is potentially very slow for large tables.
#'
#' @section Reference Implementation:
#'   Suppose that \code{id1} identifies a table in a SQL database and that
#'   \code{[i, j, by]} describes a subset/select/summarize operation using
#'   \code{data.table} syntax. The \emph{Reference Implementation} for this
#'   operation is:
#'
#'   \code{setDT(dbReadTable(conn, id1))[i, j, by]}
#'
#'   More generally, for an expression involving multiple SQL database objects
#'   and using \code{data.table} syntax, the \emph{Reference Implementation}
#'   would be to download each of these objects in their entirety, convert them
#'   to \code{data.table}s, then evaluate the expression.
#'
#'   The goal of the \pkg{dbi.table} is to generate an SQL query that produces
#'   the same results set as the Reference Implementation up to row ordering.
#'
#' @param expr
#'   an expression involving at least one \code{dbi.table} and whose result can
#'   be coerced into a \code{data.table}.
#'
#' @param envir
#'   an environment. Where to evaluate \code{expr}.
#'
#' @param ignore.row.order
#'   a logical value. This argument is passed to \code{\link{all.equal}}.
#'
#' @param verbose
#'   a logical value. When \code{TRUE}, the output from \code{all.equal} is
#'   displayed in a message when \code{all.equal} returns anything other than
#'   \code{TRUE}.
#'
#' @return
#'   a logical value.
#'
#' @examples
#'   duck <- dbi.catalog(chinook.duckdb)
#'   Album <- duck$main$Album
#'   Artist <- duck$main$Artist
#'
#'   reference.test(merge(Album, Artist, by = "ArtistId"))
#'
#' @export
reference.test <- function(expr, envir = parent.frame(),
                           ignore.row.order = TRUE, verbose = TRUE) {
  expr <- substitute(expr)

  dbits <- sapply(all.vars(expr), get0, envir = envir, simplify = FALSE)
  dbits  <- dbits[vapply(dbits, is.dbi.table, FALSE)]

  if (!length(dbits)) {
    stop("'expr' must contain at least one dbi.table")
  }

  dbits <- lapply(dbits, as.data.table)

  dbit_eval <- eval(expr, envir = envir)
  if (!(is.dbi.table(dbit_eval) || is.data.table(dbit_eval))) {
    stop("'expr' must return a 'dbi.table' or a 'data.table'")
  }

  dbit_eval <- as.data.table(dbit_eval)
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
