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

  if (inherits(id, "Id")) {
    id <- check_id(id)
  } else {
    if (is.character(id) && length(id) == 1L) {
      id <- DBI::Id(id)
    } else {
      stop("'id' argument invalid")
    }
  }

  new_dbi_table(conn, id)
}



new_dbi_table <- function(conn, id, fields = NULL) {
  if (inherits(id, "Id")) {
    id_name <- last(id@name)
  } else {
    id_name <- strsplit(as.character(id), split = ".", fixed = TRUE)[[1L]]
    id_name <- last(id_name)
  }

  if (substring(id_name, 1L, 1L) == "#") {
    id_name <- substring(id_name, 2L)
  }

  data_source <- data.frame(clause = "FROM",
                            id = I(list(id)),
                            id_name = id_name,
                            on = I(list(NULL)))

  if (is.null(fields)) {
    fields <- DBI::dbListFields(dbi_connection(conn), id)
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
as.data.table.dbi.table <- function(x, keep.rownames = FALSE, ...,
                                    n = session$max_fetch) {
  res <- try(DBI::dbSendStatement(dbi_connection(x),
                                  write_select_query(x, n)),
             silent = TRUE)

  if (inherits(res, "DBIResult")) {
    on.exit(DBI::dbClearResult(res))
  }

  if (inherits(res, "try-error")) {
    is_valid <- DBI::dbIsValid(conn <- dbi_connection(x))

    if (is_valid) {
      simple_query_works <- try(DBI::dbGetQuery(conn, "SELECT 1;"),
                                silent = TRUE)
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

    res <- DBI::dbSendStatement(dbi_connection(x), write_select_query(x, n))

    if (inherits(res, "DBIResult")) {
      on.exit(DBI::dbClearResult(res))
    } else {
      stop(res, call. = FALSE)
    }
  }

  setDT(DBI::dbFetch(res, n = n))
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
      if (nrow(i) > session$max_in_query) {
        i <- as.dbi.table(x, i, type = "temporary")
      } else {
        i <- as.dbi.table(x, i, type = "query")
      }
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
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'             Alternatively, \code{conn} may be a \code{\link{dbi.table}} or a
#'             \code{\link{dbi.catalog}}; in these cases, the connection handle
#'             is extracted from the provided object.
#'
#' @param x an \R object that can be coerced to a
#'          \code{\link[base]{data.frame}}.
#'
#' @param type a character string. Possible choices are \code{"query"}, and
#'             \code{"temporary"}. See Details.
#'
#' @details Two types of tables are provided: \emph{Temporary} (when
#'          \code{type == "query"}) and \emph{In Query}
#'          (when \code{type == "query"}). For Temporary, the data are written
#'          to a SQL temporary table and a \code{\link{dbi.table}} is returned.
#'          For In Query, the data are written into a CTE as part of the query
#'          itself. Useful when the connection does not permit creating
#'          temporary tables.
#'
#' @section Note: The temporary tables created by this function are dropped
#'                (by calling \code{\link[DBI]{dbRemoveTable}}) during garbage
#'                collection when they are no longer referenced.
#'
#' @examples
#' duck <- dbi.catalog(chinook.duckdb)
#' csql(as.dbi.table(duck, iris[1:4, 1:3], type = "query"))
#'
#' @export
as.dbi.table <- function(conn, x, type = c("temporary", "query")) {
  conn <- get_connection(conn)
  x <- as.data.frame(x)
  type <- match.arg(type)

  if (type == "temporary") {
    return(temporary_dbi_table(conn, x))
  }

  if (type == "query") {
    return(in_query_cte(conn, x))
  }

  NULL
}



temporary_dbi_table <- function(conn, x) {
  dbi_conn <- dbi_connection(conn)
  stopifnot(inherits(dbi_conn, "DBIConnection"))

  temp_name <- unique_table_name(session$tmp_base)

  if (inherits(dbi_conn, "Microsoft SQL Server")) {
    temp_name <- paste0("#", temp_name)
  }

  dev_null <- DBI::dbWriteTable(dbi_conn, temp_name, x, temporary = TRUE)

  temp_id <- DBI::Id(temp_name)
  x <- new_dbi_table(conn, temp_id, names(x))

  temp_dbi_table <- new.env(parent = emptyenv())
  temp_dbi_table$id <- temp_id
  temp_dbi_table$conn <- conn
  reg.finalizer(temp_dbi_table, finalize_temp_dbi_table)

  attr(x, "temp_dbi_table") <- temp_dbi_table
  x
}



finalize_temp_dbi_table <- function(e) {
  try(DBI::dbRemoveTable(e$conn, e$id), silent = TRUE)

  NULL
}



in_query_cte <- function(conn, data) {

  dbi_conn <- dbi_connection(conn)

  cte_name <- unique_table_name("CTE")
  id <- DBI::Id(cte_name)
  x <- new_dbi_table(conn, id, names(data))

  qnames <- DBI::dbQuoteIdentifier(dbi_conn, names(data))
  data <- mapply(DBI::dbQuoteLiteral, data, MoreArgs = list(conn = dbi_conn))

  for (j in seq_len(ncol(data))) {
    data[, j] <- paste(data[, j], "AS", qnames[[j]])
  }

  data <- apply(data, 1L, paste, collapse = ", ")
  data <- paste("SELECT", data)

  data <- DBI::SQL(paste(data, collapse = "\nUNION ALL\n"))

  ctes <- list()
  ctes[[cte_name]] <- data

  attr(x, "ctes") <- ctes
  x
}
