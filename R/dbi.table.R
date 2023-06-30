#' @import data.table



#' @importFrom utils globalVariables
globalVariables(c("."), "dbi.table", add = TRUE)



#' Create a \code{dbi.table} accessible via a \code{DBI} connection
#'
#' @description Create a \code{dbi.table} based on a SQL table.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'
#' @param id an \code{\link[DBI]{Id}} or a character string identifying a
#'           table accessible via a \code{conn}.
#'
#' @export
dbi.table <- function(conn, id) {
  hash <- register_connection(conn)

  #' @importFrom DBI Id
  #' @importFrom methods is
  if (!is(id, "Id")) {
    if (is.character(id) && length(id) == 1L) {
      id <- Id(table = id)
    } else {
      stop(sQuote("id"), " argument invalid")
    }
  }

  #' @importFrom DBI dbExistsTable
  if (!dbExistsTable(get_connection_from_hash(hash), id)) {
    stop(sQuote("id"), " not found on connection")
  }

  new_dbi_table(hash, id)
}



new_dbi_table <- function(hash, id, fields = NULL, pk = NULL) {
  conn <- get_connection_from_hash(hash)

  id_name <- id@name[["table"]]
  data_source <- data.frame(clause = "FROM",
                            id = I(list(id)),
                            id_name = id_name,
                            on = I(list(NULL)))

  if (is.null(fields)) {
    #' @importFrom DBI dbListFields
    fields <- dbListFields(conn, id)
  }

  if (is.null(pk)) {
    #' @importFrom dbi.extra dbListPrimaryKeys
    pk <- dbListPrimaryKeys(conn, id)
  }

  stopifnot(all(pk %in% fields))

  internal_name <- paste0("FN", seq_len(length(fields)))

  fields <- data.frame(internal_name = internal_name,
                       id_name = id_name,
                       field = fields)

  pk <- lapply(fields$internal_name[match(pk, fields$field)], as.name)

  x <- lapply(fields$internal_name, as.name)
  names(x) <- fields$field

  if (is.null(pk)) {
    order_by <- list()
  } else {
    order_by <- unname(sub_lang(lapply(pk, as.name), cdefs = x))
  }

  dbi_table_object(x, hash, data_source, fields, order_by = order_by)
}



dbi_table_object <- function(cdefs, hash, data_source, fields,
                             distinct = FALSE, where = list(),
                             group_by = list(), order_by = list(),
                             ctes = list()) {

  structure(cdefs, hash = hash, data_source = data_source, fields = fields,
            distinct = distinct, where = where, group_by = group_by,
            order_by = order_by, ctes = ctes, class = "dbi.table")
}



#accessor methods
get_hash <- function(x) {
  attr(x, "hash", exact = TRUE)
}



get_connection <- function(x) {
  get_connection_from_hash(get_hash(x))
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



#' @export
print.dbi.table <- function(x, ...) {
  cat(paste0("<", db_short_name(get_connection(x)), ">"),
      paste(get_data_source(x)$id_name, collapse = " + "),
      "\n")
  ans <- as.data.table(x, n = 6)
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
  #' @importFrom DBI dbSendQuery
  res <- try(dbSendQuery(get_connection(x), write_sql(x)), silent = TRUE)

  if (inherits(res, "try-error")) {
    if (!dbIsValid(get_connection(x))) {
      reconnect(get_hash(x))
    }

    res <- dbSendQuery(get_connection(x), write_sql(x))
  }

  #' @importFrom DBI dbClearResult
  on.exit(dbClearResult(res))
  #' @importFrom DBI dbFetch
  setDT(dbFetch(res, n = n))
}



#' @export
"[.dbi.table" <- function(x, i, j, by, env = parent.frame()) {

  if (missing(i) && missing(j)) {
    return(as.data.table(x))
  }

  if (missing(i)) {
    i <- NULL
  } else {
    if (inherits(try(i, silent = TRUE), "try-error")) {
      i <- substitute(i)
    }
  }

  if (missing(j)) {
    j <- NULL
  } else {
    if (inherits(try(j, silent = TRUE), "try-error")) {
      j <- substitute(j)
    }
  }

  if (missing(by)) {
    by <- NULL
  } else {
    if (inherits(try(by, silent = TRUE), "try-error")) {
      by <- substitute(by)
    }
  }

  bracket_subset(x, i, j, by, env)
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
