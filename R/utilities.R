db_short_name <- function(conn) {
  UseMethod("db_short_name")
}



#' @rawNamespace S3method(db_short_name,default,db_short_name_default)
db_short_name_default <- function(conn) {
  if (nchar(dbname <- DBI::dbGetInfo(conn)$dbname)) {
    sub("([^.]+)\\.[[:alnum:]]+$", "\\1", basename(dbname))
  } else if (is_dbi_schema(conn)) {
    get("./schema_name", conn)
  } else {
    "default_database"
  }
}



dbi_connection_package <- function(conn) {
  UseMethod("dbi_connection_package")
}



#' @rawNamespace S3method(dbi_connection_package,default,dbi_connection_package_default)
dbi_connection_package_default <- function(conn) {
  pkg <- attr(class(dbi_connection(conn)), "package", exact = TRUE)
  if (is.null(pkg)) "DBI" else pkg
}



paren <- function(x) {
  paste0("(", x, ")")
}



#' See SQL
#'
#' @description View a \code{\link{dbi.table}}'s underlying SQL query.
#'
#' @param x
#'   a \code{dbi.table}.
#'
#' @param n
#'   a single integer value. When nonnegative, limits the number of rows
#'   returned by the query to \code{n}.
#'
#' @param strict
#'   a logical value. If \code{TRUE} and \code{x} has a key, the key is
#'   appended to the ORDER BY clause.
#'
#' @return
#'   none (invisible \code{NULL}).
#'
#' @export
csql <- function(x, n = getOption("dbitable.max.fetch", 10000L),
                 strict = FALSE) {
  cat(paste0(write_select_query(x, n, isTRUE(strict)), "\n"))
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



assign_and_lock <- function(x, value, pos) {
  assign(x, value, pos)
  lockBinding(x, pos)
}



dbi_table_from_id_key <- function(catalog, id_key) {
  templates <- get("./dbi_table_templates", catalog, inherits = FALSE)
  templates[[id_key]]$constructor()
}



short_class_names <- c(integer = "int", numeric = "num",
                       character = "char", POSIXct = "POSc",
                       logical = "lgcl", integer64 = "i64")

display_class <- function(l) {
  classes <- vapply(l, function(u) class(u)[[1L]], character(1L))
  idx <- classes %in% names(short_class_names)
  classes[idx] <- short_class_names[classes[idx]]
  paste0("<", classes, ">")
}



shouldnt_print <- function(x) {
  ret <- identical(x, session$print)
  session$print <- NULL
  ret
}



copy_vector <- function(x) {
  rev(rev(x))
}



copy_data_frame <- function(x) {
  x <- as.data.frame(lapply(x, copy_vector))
  copy_vector(x)
}



rbind_f <- function(x, y, ...) {
  if (!identical(nx <- names(x), ny <- names(y))) {
    add_x <- lapply(stats::setNames(nm = setdiff(ny, nx)), function(u) NA)
    if (length(add_x)) x <- cbind(x, add_x)

    add_y <- lapply(stats::setNames(nm = setdiff(nx, ny)), function(u) NA)
    if (length(add_y)) y <- cbind(y, add_y)
  }

  base::rbind(x, y, ...)
}
