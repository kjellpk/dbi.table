check_connection <- function(conn, arg_name = "conn") {
  if (!inherits(conn, "DBIConnection")) {
    stop("invalid connection argument - '", arg_name, "' is not a ",
         "'DBI' connection", call. = FALSE)
  }

  invisible()
}



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



dbi_connection <- function(x) {
  if (is_dbi_catalog(conn <- get_connection(x))) {
    conn <- conn[[".dbi_connection"]]
  }
  conn
}