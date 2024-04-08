dbListFields <- function(conn, name, ...) {
  #' @importFrom DBI dbQuoteIdentifier dbSendQuery
  x <- dbSendQuery(conn, paste("SELECT * FROM ", dbQuoteIdentifier(conn, name)))
  #' @importFrom DBI dbClearResult
  on.exit(dbClearResult(x))
  #' @importFrom DBI dbFetch
  names(dbFetch(x, n = 0, row.names = FALSE))
}
