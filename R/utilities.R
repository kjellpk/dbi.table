db_short_name <- function(conn) {
  if (is.dbi.table(conn)) {
    conn <- dbi_connection(conn)
  }

  sub("([^.]+)\\.[[:alnum:]]+$", "\\1", basename(DBI::dbGetInfo(conn)$dbname))
}



dbi_connection_package <- function(conn) {
  if (!is.null(pkg <- attr(class(conn), "package", exact = TRUE))) {
    pkg
  } else {
    "DBI"
  }
}



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



paren <- function(x) {
  paste0("(", x, ")")
}



#' See SQL
#'
#' @description View a \code{\link{dbi.table}} as a SQL query.
#'
#' @param x a \code{\link{dbi.table}}.
#'
#' @param n a single integer value.
#'
#' @export
csql <- function(x, n = session$max_fetch) {
  cat(paste0("\n", write_select_query(x, n), "\n\n"))
}



ws <- function(n) {
  sapply(n, function(u) paste(rep(" ", u), collapse = ""))
}



pad_left <- function(x, width = 6) {
  fw <- sapply(strsplit(x, split = "[ \t\r\n]"), function(u) u[1])
  paste0(ws(width - nchar(fw)), x)
}



unique_table_name <- function(pre = "X") {
  paste0(pre, (session$table_name_counter <- 1L + session$table_name_counter))
}
