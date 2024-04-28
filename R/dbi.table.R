#' Create a \code{dbi.table} accessible via a \code{DBI} connection
#'
#' @description Create a \code{dbi.table} from an existing SQL object.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'
#' @param id an \code{\link[DBI]{Id}} or a character string identifying a
#'           table accessible via a \code{conn}.
#'
#' @export
dbi.table <- function(conn, id) {
  check_connection(conn)

  if (!inherits(id, "Id")) {
    if (is.character(id) && length(id) == 1L) {
      #' @importFrom DBI Id
      id <- Id(table = id)
    } else {
      stop("'id' argument invalid")
    }
  }

  new_dbi_table(conn, id)
}



new_dbi_table <- function(conn, id, fields = NULL) {
  id_name <- id@name[["table"]]

  data_source <- data.frame(clause = "FROM",
                            id = I(list(id)),
                            id_name = id_name,
                            on = I(list(NULL)))

  if (is.null(fields)) {
    fields <- dbListFields(dbi_connection(conn), id)
  }

  internal_name <- paste0(session$key_base, seq_len(length(fields)))

  fields <- data.frame(internal_name = internal_name,
                       id_name = id_name,
                       field = fields)

  x <- lapply(fields$internal_name, as.name)
  names(x) <- copy(fields$field)

  dbi_table_object(x, conn, data_source, fields)
}



dbi_table_object <- function(cdefs, conn, data_source, fields,
                             distinct = FALSE, where = list(),
                             group_by = list(), order_by = list(),
                             ctes = list()) {

  structure(cdefs, conn = conn, data_source = data_source, fields = fields,
            distinct = distinct, where = where, group_by = group_by,
            order_by = order_by, ctes = ctes, class = "dbi.table")
}



get_connection <- function(x) {
  attr(x, "conn", exact = TRUE)
}



dbi_connection <- function(x) {
  if (is.dbi.table(x)) {
    x <- get_connection(x)
  }

  if (is.environment(x) && !is.null(x$.dbi_connection)) {
    x <- x$.dbi_connection
  }

  stopifnot(inherits(x, "DBIConnection"))
  x
}



get_data_source <- function(x) {
  attr(x, "data_source", exact = TRUE)
}



get_fields <- function(x) {
  attr(x, "fields", exact = TRUE)
}



get_distinct <- function(x) {
  attr(x, "distinct", exact = TRUE)
}



get_where <- function(x) {
  attr(x, "where", exact = TRUE)
}



get_group_by <- function(x) {
  attr(x, "group_by", exact = TRUE)
}



get_order_by <- function(x) {
  attr(x, "order_by", exact = TRUE)
}



get_ctes <- function(x) {
  attr(x, "ctes", exact = TRUE)
}



#' Is DBI Table
#'
#' @description Function to check if an object is a \code{\link{dbi.table}}.
#'
#' @param x any \R object.
#'
#' @export
is.dbi.table <- function(x) {
  inherits(x, "dbi.table")
}



should_print <- function(x) {
  ret <- (session$print == "" || address(x) != session$print)
  session$print <- ""
  ret
}



#' @export
print.dbi.table <- function(x, ...) {
  mimics_auto_print <- "knit_print.default"
  if (!should_print(x)) {
    scs <- sys.calls()
    if (length(scs) <= 2L ||
      (length(scs) >= 3L && is.symbol(this <- scs[[length(scs) - 2L]][[1L]]) &&
      as.character(this) == "source") ||
      (length(scs) > 3L && is.symbol(this <- scs[[length(scs) - 3L]][[1L]]) &&
      as.character(this) %chin% mimics_auto_print)) {
      return(invisible(x))
    }
  }

  ans <- as.data.table(x, n = 6)

  cat(paste0("<", db_short_name(dbi_connection(x)), ">"),
      paste(get_data_source(x)$id_name, collapse = " + "),
      "\n")

  if (nrow(ans) > 5) {
    print(ans[1:5], row.names = FALSE)
    cat("---\n\n")
  } else {
    print(ans, row.names = FALSE)
  }

  invisible(x)
}



#' @export
as.data.table.dbi.table <- function(x, keep.rownames = FALSE, ..., n = -1) {
  #' @importFrom DBI dbSendStatement
  res <- try(dbSendStatement(dbi_connection(x), write_select_query(x)),
             silent = TRUE)

  if (inherits(res, "DBIResult")) {
    #' @importFrom DBI dbClearResult
    on.exit(dbClearResult(res))
  }

  if (inherits(res, "try-error")) {
    #' @importFrom DBI dbIsValid
    is_valid <- dbIsValid(conn <- dbi_connection(x))

    if (is_valid) {
      #' @importFrom DBI dbGetQuery
      simple_query_works <- try(dbGetQuery(conn, "SELECT 1;"), silent = TRUE)
      is_valid <- !inherits(simple_query_works, "try-error")
    }

    if (is_valid) {
      stop(res, call. = FALSE)
    }

    if (is.environment(e <- get_connection(x))) {
      conn <- e$.dbi_connection
      if (!is.null(recon <- attr(conn, "recon", exact = TRUE))) {
        e$.dbi_connection <- init_connection(recon)
      } else {
        stop(res, call. = FALSE)
      }
    }

    #' @importFrom DBI dbSendStatement
    res <- dbSendStatement(dbi_connection(x), write_select_query(x))

    if (inherits(res, "DBIResult")) {
      #' @importFrom DBI dbClearResult
      on.exit(dbClearResult(res))
    } else {
      stop(res, call. = FALSE)
    }
  }

  #' @importFrom DBI dbFetch
  setDT(dbFetch(res, n = n))
}



#' @export
"[.dbi.table" <- function(x, i, j, by, nomatch = NA, on = NULL) {
  x_sub <- substitute(x)
  parent <- parent.frame()

  if (!dbi_table_is_simple(x)) {
    x <- as_cte(x)
  }

  not_i <- FALSE

  if (missing(i)) {
    i <- NULL
  } else {
    i <- sub_lang(substitute(i), x, enclos = parent)

    if (is.data.frame(i)) {
      i <- as.dbi.table(i, x)
    }

    if (is_call_to(i) == "!" && is.dbi.table(i[[2L]])) {
      not_i <- TRUE
      i <- i[[2L]]
    }
  }

  stopifnot(is.null(i) || is.call(i) || is.dbi.table(i))

  if (missing(by)) {
    by <- NULL
  } else {
    by <- preprocess_by(substitute(by), x, parent, !missing(j))
  }

  if (missing(j)) {
    j <- NULL
  } else {
    j <- preprocess_j(substitute(j), x, parent, !is.null(by))
  }

  sub_on <- substitute(on)
  if (!is.null(sub_on)) {
    on <- try(eval(on, envir = parent), silent = TRUE)
    if (inherits(on, "try-error")) {
      on <- sub_on
    }
  }

  if (is.null(i) && is.null(j)) {
    return(as.data.table(x))
  }

  if (is_call_to(j) == ":=") {
    return(handle_colon_equal(x, i, j, by, parent, x_sub))
  }

  if (is.dbi.table(i)) {
    return(merge_i_dbi_table(x, i, not_i, j, by, nomatch, on, parent))
  }

  if (is.null(j) && !is.null(by)) {
    stop("cannot handle 'by' when 'j' is missing or 'NULL'", call. = FALSE)
  }

  x <- handle_i_call(x, i, parent)
  handle_j(x, j, by, parent)
}



#' @export
dim.dbi.table <- function(x) {
  c(NA_integer_, length(x))
}



#' @export
unique.dbi.table <- function(x, incomparables = FALSE, ...) {
  attr(x, "order_by") <- intersect(get_order_by(x), unname(c(x)))
  attr(x, "distinct") <- TRUE
  x
}



#' Coerce to a DBI Table
#'
#' @description Write a \code{\link[base]{data.frame}} to a temporary table on a
#'              \code{DBI} connection and return a \code{\link{dbi.table}}.
#'
#' @param x any \R object that can be coerced to a
#'          \code{\link[base]{data.frame}}.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'             Alternatively, \code{conn} may be a \code{\link{dbi.table}} or a
#'             \code{\link{dbi_database}}; in these cases, the connection handle
#'             is extracted from the provided object.
#'
#' @section Note: The temporary tables created by this function are dropped
#'                (by calling \code{\link[DBI]{dbRemoveTable}}) during garbage
#'                collection.
#'
#' @export
as.dbi.table <- function(x, conn) {
  if (!is.data.frame(x)) {
    x <- as.data.frame(x)
  }

  if (inherits(conn, "DBIConnection")) {
    dbi_conn <- conn
  } else {
    dbi_conn <- dbi_connection(conn)
    conn <- get_connection(conn)
  }

  stopifnot(inherits(dbi_conn, "DBIConnection"))

  temp_name <- unique_table_name(session$tmp_base)

  if (inherits(dbi_conn, "Microsoft SQL Server")) {
    temp_name <- paste0("#", temp_name)
  }

  #' @importFrom DBI dbWriteTable
  dev_null <- dbWriteTable(dbi_conn, temp_name, x, temporary = TRUE)

  #' @importFrom DBI Id
  temp_id <- Id(table = temp_name)
  x <- new_dbi_table(conn, temp_id)

  temp_dbi_table <- new.env(parent = emptyenv())
  temp_dbi_table$id <- temp_id
  temp_dbi_table$conn <- conn
  reg.finalizer(temp_dbi_table, finalize_temp_dbi_table)

  attr(x, "temp_dbi_table") <- temp_dbi_table
  x
}



finalize_temp_dbi_table <- function(e) {
  #' @importFrom DBI dbRemoveTable
  try(dbRemoveTable(e$conn, e$id), silent = TRUE)

  NULL
}
