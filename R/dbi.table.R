#' @import data.table



#' @importFrom utils globalVariables
globalVariables(c("COUNT", "."), "dbi.table", add = TRUE)



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

  pk <- fields$internal_name[match(pk, fields$field)]

  x <- lapply(fields$internal_name, as.name)
  names(x) <- fields$field

  dbi_table_object(x, hash, data_source, fields, pk)
}



dbi_table_object <- function(column_definitions, connection_hash, data_source,
                             fields, sorted, distinct = FALSE, where = list(),
                             group_by = list(), order_by = list(),
                             ctes = list()) {

  a <- list(names = names(column_definitions), hash = connection_hash,
            data_source = data_source, fields = fields, sorted = sorted,
            distinct = distinct, where = where, by = group_by, order = order_by,
            ctes = ctes, class = "dbi.table")

  attributes(column_definitions) <- a
  column_definitions
}



#accessor methods
get_hash <- function(x) {
  stopifnot(is.dbi.table(x))
  attr(x, "hash", exact = TRUE)
}



get_connection <- function(x) {
  stopifnot(is.dbi.table(x))
  get_connection_from_hash(get_hash(x))
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
      paste(attr(x, "data_source", exact = TRUE)$id_name, collapse = " + "),
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
  attr(x, "distinct") <- TRUE
  x
}
