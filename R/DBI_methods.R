dbExecute_dbi_table_pkg <- function(conn, statement, ...) {
  DBI::dbExecute(dbi_connection(conn), statement, ...)
}



dbSendStatement_dbi_table_pkg <- function(conn, statement, ...,
                                          n = session$max_fetch) {
  DBI::dbSendStatement(dbi_connection(conn), write_select_query(conn, n))
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
#' DBI Methods for \code{dbi.table}s
#'
#' Call DBI methods using the underlying DBI connection.
#'
#' @param conn
#'   a \code{\link{dbi.catalog}}, \code{dbi.schema}, or \code{\link{dbi.table}}.
#'
#' @param statement
#'   a \code{\link[DBI]{SQL}} object.
#'
#' @param ...
#'   other parameters passed on to methods.
#' 
#' @param n
#'   an integer value. A nonnegative value limits the number of records returned
#'   by the query. A negative value omits the LIMIT (or TOP) clause entirely.
#'
#' @seealso
#'   \code{\link[DBI]{dbExecute}}, \code{\link[DBI]{dbSendStatement}}
#'
#' @examples
#' duck <- dbi.catalog(chinook.duckdb)
#' dbExecute(duck, DBI::SQL("INSTALL httpfs;"))
#' dbExecute(duck, DBI::SQL("LOAD httpfs;"))
#'
#' @docType methods
#' @rdname DBI-methods
#' @aliases dbExecute,dbi.catalog,SQL-method
#' @importFrom DBI dbExecute SQL
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.catalog", "SQL"),
          definition = dbExecute_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbExecute,dbi.schema,SQL-method
#' @importFrom DBI dbExecute SQL
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.schema", "SQL"),
          definition = dbExecute_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbExecute,dbi.table,SQL-method
#' @importFrom DBI dbExecute SQL
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.table", "SQL"),
          definition = dbExecute_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbSendStatement,dbi.table,missing-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = c("dbi.table", "missing"),
          definition = dbSendStatement_dbi_table_pkg)
