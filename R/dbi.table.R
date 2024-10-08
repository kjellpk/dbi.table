#' Create and Manipulate \code{dbi.table} Objects
#'
#' @description
#'   A \code{dbi.table} is an SQL query that can be manipulated using
#'   \code{\link[data.table]{data.table}}-like syntax. The constructor function
#'   \code{dbi.table} creates a query that selects all rows and and all columns
#'   from a database object (i.e., \code{SELECT * FROM id}), and the brackets
#'   method creates subsets, summaries, etc.
#'
#' @param conn
#'   A connection handle returned by \code{\link[DBI]{dbConnect}}.
#'   Alternatively, a \code{\link{dbi.catalog}} or another
#'   \code{dbi.table}, in which case the new \code{dbi.table} will share the
#'   same connection.
#'
#' @param id
#'   An \code{Id}, a character string (which will be converted to
#'   an \code{Id} by \code{\link[DBI]{Id}}), or a \code{\link[DBI]{SQL}} object
#'   (advanced) identifying a database object that exists on \code{conn}.
#'
#' @return
#'   A \code{dbi.table} (an S3 object). A \code{dbi.table} is a list of
#'   \emph{expressions} representing the \code{SELECT} clause of an SQL query as
#'   well as a set of attributes containing metadata needed for the rest of the
#'   SQL query.
#'
#' @seealso
#'   \itemize{
#'     \item \code{\link{as.data.table}} to retrieve the \emph{results set} as
#'           a \code{data.table},
#'     \item \code{\link{csql}} to see the underlying SQL query.
#'   }
#'
#' @examples
#'   duck <- chinook.duckdb()
#'
#'   Album <- dbi.table(duck, DBI::Id(table_name = "Album"))
#'
#'   # use same connection as 'Album'; 'id' is a length-2 character interpreted
#'   # as a table_schema and table_name
#'   Artist <- dbi.table(Album, c("main", "Artist"))
#'
#'   # 'id' can also be 'SQL'
#'   Genre <- dbi.table(Album, DBI::SQL("chinook_duckdb.main.Genre"))
#'
#'   # the print method displays a 5 row preview
#'   Album
#'
#'   # use the brackets method to subset the dbi.table
#'   Album[AlbumId < 5, .(Title, nchar = paste(nchar(Title), "characters"))]
#'
#'   # use csql to see the underlying SQL query
#'   csql(Album[AlbumId < 5, #WHERE
#'              .(Title, #SELECT
#'                nchar = paste(nchar(Title), "characters"))])
#'
#'   \dontshow{DBI::dbDisconnect(duck)}
#'
#' @export
dbi.table <- function(conn, id) {
  conn <- get_connection(conn)

  if (inherits(id, "SQL")) {
    if (length(id) != 1L) {
      stop("'id' is not length 1")
    }
  } else if (is.character(id)) {
    id <- DBI::Id(id)
  }

  if (inherits(id, "Id")) {
    id <- check_id(id)
  }

  new_dbi_table(conn, id)
}



new_dbi_table <- function(conn, id, fields = NULL) {
  if (inherits(id, "Id")) {
    id_name <- last(id@name)
  } else {
    id_name <- DBI::dbUnquoteIdentifier(dbi_connection(conn), id)[[1L]]@name
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



#' @rdname as.dbi.table
#' @name as.dbi.table
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

  if (nrow(ans) > 5L) {
    print(ans[1:5], row.names = FALSE)
    cat(" ---\n")
  } else if (nrow(ans) > 0L) {
    print(ans, row.names = FALSE)
  } else {
    m <- paste("Empty dbi.table (0 rows and", length(ans), "cols):",
               paste(names(ans), collapse = ","))
    if (nchar(m) > (width <- getOption("width", 80L))) {
      m <- paste0(substring(m, 1L, width - 3L), "...")
    }
    cat(m, "\n", sep = "")
  }

  invisible(x)
}



#' @export
as.data.table.dbi.table <- function(x, keep.rownames = FALSE, ...,
                                    n = getOption("dbi_table_max_fetch",
                                                  10000L)) {
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



#' @rdname dbi.table
#'
#' @param x
#'   A \code{dbi.table}.
#'
#' @param i
#'   A logical expression of the columns of \code{x}, a \code{dbi.table},
#'   or a \code{data.frame}. Use \code{i} to select a subset of the rows of
#'   \code{x}. Note: unlike \code{data.table}, \code{i} \emph{cannot} be a
#'   vector.
#'
#'   When \code{i} is a logical expression, the rows where the expression is
#'   \code{TRUE} are returned. If the expression contains a symbol \code{foo}
#'   that is not a column name of \code{x} but that is present in the calling
#'   scope, then the value of \code{foo} will be substituted into the expression
#'   if \code{foo} is a scalar, or if \code{foo} is a vector and is the
#'   right-hand-side argument to \code{\%in\%} or \code{\%chin\%} (substitution
#'   occurs when the \code{[} method is evaluated).
#'
#'   When \code{i} inherits from \code{data.frame}, it is coerced to a
#'   \code{dbi.table}.
#'
#'   When \code{i} is a \code{dbi.table}, the rows of \code{x} that match
#'   (according to the condition specificed in \code{on}) the rows
#'   of \code{i} are returned. When \code{nomatch == NA}, all rows of \code{i}
#'   are returned (right outer join); when \code{nomatch == NULL}, only the rows
#'   of \code{i} that match a row of \code{x} are returned (inner join).
#'
#' @param j
#'   A list of expressions, a literal character vector of column names of
#'   \code{x}, an expression of the form \code{start_name:end_name}, or a
#'   literal numeric vector of integer values indexing the columns of \code{x}.
#'   Use \code{j} to select (and optionally, transform) the columns of \code{x}.
#'
#' @param by
#'   A list of expressions, a literal character vector of column names of
#'   \code{x}, an expression of the form \code{start_name:end_name}, or a
#'   literal numeric vector of integer values indexing the columns of \code{x}.
#'   Use \code{by} to control grouping when evaluating \code{j}.
#'
#' @param nomatch
#'   Either \code{NA} or \code{NULL}.
#'
#' @param on
#'   \itemize{
#'     \item An unnamed character vector, e.g., \code{x[i, on = c("a", "b")]},
#'           used when columns \code{a} and \code{b} are common to both \code{x}
#'           and \code{i}.
#'
#'     \item Foreign key joins: As a named character vector when the join
#'           columns have different names in \code{x} and \code{i}. For example,
#'           \code{x[i, on = c(x1 = "i1", x2 = "i2")]} joins \code{x} and
#'           \code{i} by matching columns \code{x1} and \code{x2} in \code{x}
#'           with columns \code{i1} and \code{i2} in \code{i}, respectively.
#'
#'     \item Foreign key joins can also use the binary operator \code{==}, e.g.,
#'           \code{x[i, on = c("x1 == i1", "x2 == i2")]}.
#'
#'     \item It is also possible to use \code{.()} syntax as
#'           \code{x[i, on = .(a, b)]}.
#'
#'     \item Non-equi joins using binary operators \code{>=}, \code{>},
#'           \code{<=}, \code{<} are also possible, e.g.,
#'           \code{x[i, on = c("x >= a", "y <= b")]}, or
#'           \code{x[i, on = .(x >= a, y <= b)]}.
#'   }
#'
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
      if (nrow(i) > getOption("dbi_table_max_in_query", 500L)) {
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



#' Coerce to DBI Table
#'
#' Test whether an object is a \code{dbi.table}, or coerce it if possible.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'             Alternatively, \code{conn} may be a \code{\link{dbi.table}} or a
#'             \code{\link{dbi.catalog}}; in these cases, the connection handle
#'             is extracted from the provided object.
#'
#' @param x any \R object.
#'
#' @param type a character string. Possible choices are \code{"auto"},
#'             \code{"query"}, and \code{"temporary"}. See Details. The default
#'             \code{"auto"} uses \emph{In Query} tables when \code{x} has 500
#'             or fewer rows or when creating a temporary table on the database
#'             fails.
#'
#' @details Two types of tables are provided: \emph{Temporary} (when
#'          \code{type == "temporary"}) and \emph{In Query}
#'          (when \code{type == "query"}). For \emph{Temporary}, the data are
#'          written to a SQL temporary table and the associated
#'          \code{dbi.table} is returned. For \emph{In Query}, the data are
#'          written into a CTE as part of the query itself - useful when the
#'          connection does not permit creating temporary tables.
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
as.dbi.table <- function(conn, x, type = c("auto", "query", "temporary")) {
  conn <- get_connection(conn)
  x <- as.data.frame(x)
  n <- nrow(x)
  type <- match.arg(type)

  if (type == "temporary") {
    return(temporary_dbi_table(conn, x))
  }

  if (type == "query") {
    return(in_query_cte(conn, x))
  }

  if (n > getOption("dbi_table_max_in_query", 500L)) {
    if (is_dbi_catalog(conn) && isTRUE(conn[[".temporary_table_denied"]])) {
      warning("writing ", n, " row data.frame into query statement since ",
              "permission to create temporary table was denied - processing ",
              "will fail if statement is too large")
      return(in_query_cte(conn, x))
    }

    if (is.dbi.table(tmp <- try(temporary_dbi_table(conn, x), silent = TRUE))) {
      return(tmp)
    }

    if (is_dbi_catalog(conn)) {
      conn[[".temporary_table_denied"]] <- TRUE
    }

    warning("writing ", n, " row data.frame into query statement since ",
            "permission to create temporary table was denied - processing ",
            "will fail if statement is too large")
  }

  in_query_cte(conn, x)
}



temporary_dbi_table <- function(conn, x) {
  dbi_conn <- dbi_connection(conn)
  stopifnot(inherits(dbi_conn, "DBIConnection"))

  temp_name <- unique_table_name(session$tmp_base)

  if (inherits(dbi_conn, "Microsoft SQL Server")) {
    temp_name <- paste0("#", temp_name)
  }

  if (!DBI::dbWriteTable(dbi_conn, temp_name, x, temporary = TRUE)) {
    stop("could not create temporary table - permission denied")
  }

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
  data <- as.data.frame(data)

  for(j in which(vapply(data, is.factor, FALSE))) {
    data[[j]] <- as.character(data[[j]])
  }

  cte_name <- unique_table_name("IN_QUERY_CTE")
  id <- DBI::Id(cte_name)
  x <- new_dbi_table(conn, id, names(data))

  qnames <- DBI::dbQuoteIdentifier(dbi_conn, names(data))
  col_modes <- vapply(data, storage.mode, "")

  for (j in seq_along(data)) {
    tmp <- mapply("call",
                  name = paste("as", col_modes[[j]], sep = "."),
                  data[[j]],
                  SIMPLIFY = FALSE,
                  USE.NAMES = FALSE)
    tmp <- translate_sql_(tmp, dbi_conn)
    data[[j]] <- DBI::SQL(paste(tmp, "AS", qnames[[j]]))
  }

  data <- apply(data, 1L, paste, collapse = ", ")
  data <- paste("SELECT", data)

  data <- DBI::SQL(paste(data, collapse = "\nUNION ALL\n"))

  ctes <- list()
  ctes[[cte_name]] <- data

  attr(x, "ctes") <- ctes
  x
}
