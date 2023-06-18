db_short_name <- function(x, pkg = FALSE) {
  if (is_hash(x)) {
    x <- get_connection_from_hash(x)
  }

  #' @importFrom DBI dbGetInfo
  #' @importFrom tools file_path_sans_ext
  n <- file_path_sans_ext(basename(dbGetInfo(x)$dbname))

  if (pkg && !is.null(pkg <- attr(class(x), "package", exact = TRUE))) {
    n <- paste(pkg, n, sep = ":")
  }

  n
}



paren <- function(x) {
  paste0("(", x, ")")
}



#' See SQL
#'
#' @description View a \code{\link{dbi.table}} as a SQL query.
#'
#' @param x a \code{\link{dbi.table}}.
#' @export 
csql <- function(x) {
  cat(paste0("\n", write_sql(x), "\n\n"))
}



ws <- function(n) {
  sapply(n, function(u) paste(rep(" ", u), collapse = ""))
}



pad_left <- function(x, width = 6) {
  fw <- sapply(strsplit(x, split = "[ \t\r\n]"), function(u) u[1])
  paste0(ws(width - nchar(fw)), x)
}



e <- new.env()

unique_table_name <- function(pre = "X") {
  if (is.null(e$u_counter)) {
    e$u_counter <- 0L
  }

  paste0(pre, (e$u_counter <- 1L + e$u_counter))
}
