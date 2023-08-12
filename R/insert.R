#' Coerce to a DBI Table
#'
#' @description Write a \code{\link[base]{data.frame}} to temporary table and
#'              return a \code{\link{dbi.table}}.
#'
#' @param x an \R object coercable to a \code{\link[base]{data.frame}}.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'             Alternatively, \code{conn} may be a \code{\link{dbi.table}}; in
#'             this case the handle associated with \code{conn} is used.
#'
#' @param row.names see \code{row.names} in \code{\link[DBI]{dbWriteTable}}.
#'
#' @export
as.dbi.table <- function(x, conn, row.names = FALSE) {
  if (!is.data.frame(x)) {
    stop(sQuote("x"), " is not a data.frame")
  }

  table_name <- gsub(".", "_", deparse(substitute(x)), fixed = TRUE)

  if (is.dbi.table(conn)) {
    conn <- get_hash(conn)
  }

  if (is_hash(conn)) {
    conn <- get_connection_from_hash(conn)
  }

  #' @importFrom DBI dbExistsTable
  if (dbExistsTable(conn, table_name)) {
    table_name <- unique_table_name()
  }

  #' @importFrom DBI dbWriteTable
  status <- dbWriteTable(conn, table_name, as.data.frame(x),
                         row.names = row.names, temporary = TRUE)

  stopifnot(status)

  dbi.table(conn, table_name)
}
