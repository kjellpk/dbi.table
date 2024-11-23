db_short_name <- function(conn) {
  conn <- dbi_connection(conn)

  info <- DBI::dbGetInfo(conn)
  if (nchar(dbname <- info$dbname)) {
    short_name <- sub("([^.]+)\\.[[:alnum:]]+$", "\\1", basename(dbname))
  } else if (nchar(host <- info$host)) {
    short_name <- host
  } else {
    short_name <- "default_catalog"
  }

  short_name
}



dbi_connection_package <- function(conn) {
  if (!is.null(pkg <- attr(class(conn), "package", exact = TRUE))) {
    pkg
  } else {
    "DBI"
  }
}



check_id <- function(id) {
  if ((n <- length(id@name)) > 3L) {
    stop("'id' has more than 3 components", call. = FALSE)
  }

  valid_names <- last(c("table_catalog", "table_schema", "table_name"), n)

  if (is.null(id_names <- names(id@name))) {
    names(id@name) <- valid_names
  } else {
    for (i in which(id_names != valid_names)) {
      warning("renaming 'id' component: '", id_names[i],
              "'; new name: '", valid_names[i], "'")
    }
    names(id@name) <- valid_names
  }

  id
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
#' @export
csql <- function(x, n = getOption("dbi_table_max_fetch", 10000L)) {
  cat(paste0(write_select_query(x, n), "\n"))
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
