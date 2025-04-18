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
  conn <- dbi_connection(conn)
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

  valid_names <- c("table_name", "table_schema", "table_catalog")[seq_len(n)]
  valid_names <- rev(valid_names)

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
  return(TRUE)
}



make_install_function <- function(catalog, id, fields, column_names, key) {
  function(x) {
    if (missing(x)) {
      dbi_table <- new_dbi_table(catalog, id, fields, key)
      names(dbi_table) <- copy_vector(column_names)
      return(dbi_table)
    }

    stop("'dbi.table' cannot be modified", call. = FALSE)
  }
}



install_in_schema <- function(x, catalog, id, fields, column_names, key, schema) {
  FUN <- make_install_function(catalog, id, fields, column_names, key)
  makeActiveBinding(x, FUN, schema)
  lockBinding(x, schema)
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
