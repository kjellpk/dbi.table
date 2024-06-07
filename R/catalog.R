#' Create a \code{dbi.catalog}
#'
#' A \code{dbi.catalog} is an \code{environment} with the class
#' attribute set to \code{"dbi.catalog"}.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}} or
#'             a zero-argument function that returns a connection handle.
#'
#' @return \code{dbi.catalog} returns a \code{dbi.catalog} (internally an
#'         \code{\link[base]{environment}} with the class attribute set to
#'         \code{"dbi.catalog"}).
#'
#' @examples
#' # chinook.sqlite is a zero-argument function that returns a DBI handle
#' db <- dbi.catalog(chinook.duckdb)
#'
#' # a dbi.catalog corresponds to a catalog - list the schemas
#' ls(db)
#'
#' # list the tables in the schema 'main'
#' ls(db$main)
#'
#' @export
dbi.catalog <- function(conn) {
  check_connection(conn <- init_connection(conn))
  db <- new.env(parent = emptyenv())

  db$.dbi_connection <- conn
  class(db) <- "dbi.catalog"

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(db, dbi.catalog_disconnect, onexit = TRUE)
  }

  db$information_schema <- information_schema(conn, db)

  objs <- list_database_objects(conn, db$information_schema)

  for (i in seq_len(nrow(objs))) {
    obj <- objs[i]
    if (is.null(schema <- obj$schema)) {
      schema <- "main"
    }

    if (tolower(schema) == "information_schema") next

    if (is.null(db[[schema]])) {
      db[[schema]] <- new.env(parent = emptyenv())
    }

    table <- obj$table
    id <- obj$table_id[[1L]]
    column_names <- obj$column_names[[1L]]

    db[[schema]][[table]] <- new_dbi_table(db, id, column_names)
  }

  for (schema in ls(db)) {
    db[[schema]][[".."]] <- db
  }

  db
}



dbi.catalog_disconnect <- function(e) {
  on.exit(rm(list = ".dbi_connection", envir = e))
  #' @importFrom DBI dbDisconnect
  try(dbDisconnect(e[[".dbi_connection"]]), silent = TRUE)
}



#' @export
print.dbi.catalog <- function(x, ...) {
  conn <- x$.dbi_connection
  name <- paste(dbi_connection_package(conn), db_short_name(conn), sep = "::")

  cat(paste0("<", name, ">"), "\n")

  invisible(x)
}



is_dbi_catalog <- function(x) {
  inherits(x, "dbi.catalog")
}



list_database_objects <- function(conn, info) {
  if (is.null(columns <- info$.init_cols)) {
    columns <- info$columns
  } else {
    rm(".init_cols", pos = info)
  }

  columns <- as.data.table(columns)

  id_columns <- c(catalog = "table_catalog",
                  schema = "table_schema",
                  table = "table_name")
  id_columns <- id_columns[id_columns %chin% names(columns)]

  columns[, list(table_id = list(Id(unlist(.BY))),
                 column_names = list(column_name)),
          by = id_columns]
}
