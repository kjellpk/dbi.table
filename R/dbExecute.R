dbExecute_dbi_table_pkg <- function(conn, statement, ...) {
  DBI::dbExecute(dbi_connection(conn), statement, ...)
}



#' @importFrom methods setOldClass
#' @export
setOldClass("dbi.catalog")



#' @importFrom methods setOldClass
#' @export
setOldClass("dbi.schema")



#' @importFrom methods setOldClass
#' @export
setOldClass("dbi.table")

################################################################################
#' Change database state
#'
#' Execute a statement and returns the number of rows affected.
#'
#' @param conn a \code{\link{dbi.catalog}}, \code{dbi.schema}, or
#'             \code{\link{dbi.table}}.
#'
#' @param statement a character string containing SQL.
#'
#' @param ... other parameters passed on to methods.
#'
#' @details This function calls \code{\link[DBI]{dbExecute}} using the
#'          connection handle extracted from a \code{\link{dbi.catalog}},
#'          \code{dbi.schema}, or \code{\link{dbi.table}}.
#'
#' @seealso \code{\link[DBI]{dbExecute}}
#'
#' @return a scalar numeric that specifies the number of rows affected by the
#'         statement.
#'
#' @examples
#' duck <- dbi.catalog(chinook.duckdb)
#' dev.null <- dbExecute(duck, "INSTALL httpfs;")
#' dev.null <- dbExecute(duck, "LOAD httpfs;")
#'
#' @docType methods
#' @rdname dbExecute-methods
#' @aliases dbExecute,dbi.catalog,ANY-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.catalog", "ANY"),
          definition = dbExecute_dbi_table_pkg)



#' @rdname dbExecute-methods
#' @aliases dbExecute,dbi.schema,ANY-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.schema", "ANY"),
          definition = dbExecute_dbi_table_pkg)



#' @rdname dbExecute-methods
#' @aliases dbExecute,dbi.table,ANY-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.table", "ANY"),
          definition = dbExecute_dbi_table_pkg)
