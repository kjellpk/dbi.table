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
#' # chinook.duckdb is a zero-argument function that returns a DBI handle
#' (db <- dbi.catalog(chinook.duckdb))
#'
#' # a dbi.catalog corresponds to a catalog - list the schemas
#' ls(db)
#'
#' # list the tables in the schema 'main'
#' ls(db$main)
#'
#' @export
dbi.catalog <- function(conn) {
  conn <- get_connection(init_connection(conn))
  db <- new.env(parent = emptyenv())

  db$.dbi_connection <- conn
  class(db) <- "dbi.catalog"

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(db, dbi.catalog_disconnect, onexit = TRUE)
  }

  db$information_schema <- information_schema(conn, db)
  objs <- list_database_objects(conn, db$information_schema)

  dbname <- DBI::dbGetInfo(conn)$dbname

  if (nchar(dbname)) {
    if (nrow(schema <- objs[schema == dbname])) {
      objs <- schema
    } else if (nrow(catalog <- objs[catalog == dbname])) {
      objs <- catalog
    }
  }

  for (i in seq_len(nrow(objs))) {
    obj <- objs[i]
    if (is.null(table_schema <- obj$table_schema)) {
      table_schema <- "main"
    }

    if (tolower(table_schema) == "information_schema") next

    if (is.null(db[[table_schema]])) {
      db[[table_schema]] <- new_schema(name = table_schema, catalog = db)
    }

    table_name <- obj$table_name
    id <- obj$table_id[[1L]]
    column_names <- obj$column_names[[1L]]

    db[[table_schema]][[table_name]] <- new_dbi_table(db, id, column_names)
  }

  db
}



dbi.catalog_disconnect <- function(e) {
  on.exit(rm(list = ".dbi_connection", envir = e))
  try(DBI::dbDisconnect(e[[".dbi_connection"]]), silent = TRUE)
}



#' @export
print.dbi.catalog <- function(x, ...) {
  conn <- dbi_connection(x)
  name <- paste(dbi_connection_package(conn), db_short_name(conn), sep = "::")
  desc <- paste(length(lsx <- ls(x)), "schemas containing",
                sum(as.integer(eapply(x, function(u) length(ls(u))))),
                "objects")
  if ((n <- length(lsx)) > 30L) {
    lsx <- lsx[1L:30L]
    n_schemas_omitted <- n - 30L
  } else {
    n_schemas_omitted <- 0L
  }

  cat("<Database Catalog>", name, paste0("(", desc, ")"), "\n")
  print(lsx)

  if (n_schemas_omitted > 0L) {
    cat("(an additional", n_schemas_omitted, "schemas were not displayed -",
        "use 'ls' to list all schemas)\n")
  }

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

  id_columns <- c("table_catalog", "table_schema", "table_name")
  id_columns <- id_columns[id_columns %chin% names(columns)]

  columns[, list(table_id = list(DBI::Id(unlist(.BY))),
                 column_names = list(column_name)),
          by = id_columns]
}



new_schema <- function(name, catalog) {
  init_schema <- init_schema(new.env(parent = emptyenv()), name, catalog)
}



init_schema <- function(e, name, catalog) {
  name <- as.character(name)[[1L]]
  stopifnot(is_dbi_catalog(catalog))

  assign("./schema_name", name, pos = e)
  assign("../catalog", catalog, pos = e)

  class(e) <- "dbi.schema"
  e
}



is_dbi_schema <- function(x) {
  inherits(x, "dbi.schema")
}



#' @export
print.dbi.schema <- function(x, ...) {
  desc <- paste("contains", length(lsx <- ls(x)), "objects")

  if ((n <- length(lsx)) > 30L) {
    lsx <- lsx[1L:30L]
    n_objects_omitted <- n - 30L
  } else {
    n_objects_omitted <- 0L
  }

  cat("<Database Schema>", x[["./schema_name"]], paste0("(", desc, ")\n"))
  print(lsx)

  if (n_objects_omitted > 0L) {
    cat("(an additional", n_objects_omitted, "objects were not displayed -",
        "use 'ls' to list all objects in schema)\n")
  }

  invisible(x)
}
