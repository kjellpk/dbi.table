init_connection <- function(conn) {
  if (is.function(conn)) {
    recon_fun <- conn
    conn <- conn()
    attr(conn, "recon") <- recon_fun
  }

  conn
}



get_connection <- function(x) {
  UseMethod("get_connection")
}



#' @export
get_connection.dbi.catalog <- function(x) {
  x
}



#' @export
get_connection.dbi.schema <- function(x) {
  x[["../catalog"]]
}



#' @export
get_connection.dbi.table <- function(x) {
  attr(x, "conn", exact = TRUE)
}



#' @export
get_connection.DBIConnection <- function(x) {
  x
}



#' @export
get_connection.default <- function(x) {
  stop("invalid connection argument - 'conn' is not a 'DBI' connection",
       call. = FALSE)

  invisible()
}



get_catalog <- function(x) {
  if (is_dbi_catalog(x <- get_connection(x))) x else NULL
}
