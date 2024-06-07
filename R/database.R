#' Create a \code{dbi_database}
#'
#' Use \code{dbi_database} to create a \code{dbi_database},
#' \code{list_db_objects} to get a list of database objects (e.g., tables and
#' views) that can be added to the \code{dbi_database}, and
#' \code{add_db_objects} to add one or more of these objects. See Details and
#' Examples.
#'
#' A \code{dbi_database} is an \code{environment} with the class
#' attribute set to \code{"dbi_database"}. Initially, the \code{dbi_database}
#' contains only the \code{information_schema}. When a database object
#' (e.g., a table or view identified by an \code{\link[DBI]{Id}}) is
#' added to the \code{dbi_database} using \code{add_db_objects}, a
#' \code{\link{dbi.table}} is created in the \code{dbi_database}. Since the
#' \code{dbi_database} is an envionment, the added \code{\link{dbi.table}} can
#' be accessed using the \code{$} operator. Further, one can use
#' \code{\link[base]{ls}} and \code{\link[base]{objects}} to list the
#' \code{\link{dbi.table}}s in the \code{dbi_database}. See Examples.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}} or
#'             a zero-argument function that returns a connection handle.
#'
#'
#' @return \code{dbi_database} returns a \code{dbi_database} (internally an
#'         \code{\link[base]{environment}} with the class attribute set to
#'         \code{"dbi_database"}).
#'
#' @examples
#' # chinook.sqlite is a zero-argument function that returns a DBI handle
#' db <- dbi_database(chinook.duckdb)
#'
#' # a dbi_database corresponds to a catalog - list the schemas
#' ls(db)
#'
#' # list the tables in the schema 'main'
#' ls(db$main)
#'
#' @export
dbi_database <- function(conn) {
  check_connection(conn <- init_connection(conn))
  db <- new.env(parent = emptyenv())

  db$.dbi_connection <- conn
  class(db) <- "dbi_database"

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(db, dbi_database_disconnect, onexit = TRUE)
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



dbi_database_disconnect <- function(e) {
  on.exit(rm(list = ".dbi_connection", envir = e))
  #' @importFrom DBI dbDisconnect
  try(dbDisconnect(e[[".dbi_connection"]]), silent = TRUE)
}



#' @export
print.dbi_database <- function(x, ...) {
  conn <- x$.dbi_connection
  name <- paste(dbi_connection_package(conn), db_short_name(conn), sep = "::")

  cat(paste0("<", name, ">"), "\n")

  invisible(x)
}



is_dbi_database <- function(x) {
  inherits(x, "dbi_database")
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
