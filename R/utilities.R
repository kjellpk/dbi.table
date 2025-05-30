db_short_name <- function(conn) {
  UseMethod("db_short_name")
}



#' @rawNamespace S3method(db_short_name,default,db_short_name_default)
db_short_name_default <- function(conn) {
  if (nchar(dbname <- DBI::dbGetInfo(conn)$dbname)) {
    sub("([^.]+)\\.[[:alnum:]]+$", "\\1", basename(dbname))
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



find_environment <- function(x, mode = "any", class = NULL,
                             envir = parent.frame()) {
  if (identical(envir, emptyenv())) {
    return(NULL)
  }

  if (!is.character(x) && !is.symbol(x <- substitute(x))) {
    return(NULL)
  }

  x <- as.character(x)[[1L]]

  if (!is.null(obj <- get0(x, envir, mode, inherits = FALSE))) {
    if (is.null(class)) {
      return(envir)
    } else if (inherits(obj, class)) {
      return(envir)
    }
  }

  find_environment(x, mode, class, parent.env(envir))
}



assign_and_lock <- function(x, value, pos) {
  assign(x, value, pos)
  lockBinding(x, pos)
}



get_table_schema_from_id <- function(catalog, id) {
  cts <- catalog[["./tables_schema"]]
  i <- match(list(id), cts$id)
  cts <- cts[i, ]

  if (!length(fields <- cts[[1L, "fields"]])) {
    fields <- DBI::dbListFields(catalog, id)
    catalog[["./tables_schema"]][[i, "fields"]] <- fields
  }

  list(catalog = catalog,
       id = id,
       column_names = fields,
       key = cts[[1L, "key"]])
}



new_dbi_table_from_id <- function(catalog, id) {
  if (is.null(s <- get_table_schema_from_id(catalog, id))) {
    return(NULL)
  }

  new_dbi_table(catalog, id, copy_vector(s$column_names), copy_vector(s$key))
}



new_active_dbi_table <- function(catalog, id) {
  function(x) {
    if (missing(x)) {
      return(new_dbi_table_from_id(catalog, id))
    }

    stop("this dbi.table cannot be modified", call. = FALSE)
  }
}



install_active_dbi_table <- function(catalog, schema, name, id) {
  fn <- new_active_dbi_table(catalog, id)
  makeActiveBinding(name, fn, schema)
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
