#' Create a \code{dbi.catalog}
#'
#' A \code{dbi.catalog} represents a database catalog.
#'
#' @param conn
#'   a connection handle returned by \code{\link[DBI]{dbConnect}} or a
#'   zero-argument function that returns a connection handle.
#'
#' @param schemas
#'   a character vector of distinct schema names. These schemas will be loaded
#'   into the \code{dbi.catalog}. The default \code{schemas = NULL} loads all
#'   schemas in the catalog.
#'
#' @return
#'   a \code{dbi.catalog}.
#'
#' @examples
#' # chinook.duckdb is a zero-argument function that returns a DBI handle
#' (db <- dbi.catalog(chinook.duckdb))
#'
#' # list schemas
#' ls(db)
#'
#' # list the tables in the schema 'main'
#' ls(db$main)
#'
#' @export
dbi.catalog <- function(conn, schemas = NULL) {
  conn <- init_connection(conn)

  catalog <- new.env(parent = emptyenv())
  assign("./dbi_connection", conn, catalog)

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(catalog, dbi.catalog_disconnect, onexit = TRUE)
  }

  class(catalog) <- "dbi.catalog"

  columns <- tables_schema(dbi_connection(catalog))
  information_schema(catalog, columns)

  if (is.null(columns$table_schema)) {
    schema_names <- columns$table_schema <- "main"
  } else {
    schema_names <- setdiff(unique(columns$table_schema), "information_schema")
  }

  if (is.null(schemas)) {
    schemas <- schema_names
  } else {
    schemas <- intersect(as.character(schemas), schema_names)
  }

  names(schemas) <- schemas
  schemas <- lapply(schemas, new_schema, catalog = catalog)

  install_from_columns(columns, schemas, catalog)

  catalog
}



ID_COLUMNS <- c("table_catalog", "table_schema", "table_name")
VALUE_COLUMNS <- c("column_name", "ordinal_position", "pk_ordinal_position")

install_from_columns <- function(columns, schemas, catalog, to_lower = FALSE) {
  schema_names <- names(schemas)
  id_cols <- intersect(ID_COLUMNS, names(columns))

  columns <- subset(columns,
                    subset = table_schema %in% schema_names,
                    select = c(id_cols, VALUE_COLUMNS))

  tables <- split(columns, columns[, id_cols], drop = TRUE)

  tables <- lapply(tables, function(u) {
    id <- DBI::Id(unlist(u[1L, id_cols]))
    fields <- u$column_name[order(u$ordinal_position)]
    key_idx <- !is.na(u$pk_ordinal_position)
    key <- u$column_name[key_idx][u$pk_ordinal_position[key_idx]]

    if (to_lower) {
      table_schema <- tolower(u$table_schema[[1L]])
      table_name <- tolower(u$table_name[[1L]])
      column_names <- tolower(fields)
    } else {
      table_schema <- u$table_schema[[1L]]
      table_name <- u$table_name[[1L]]
      column_names <- fields
    }

    schema <- schemas[[table_schema]]

    install_in_schema(table_name, catalog, id, fields, column_names, key, schema)
  })

  invisible()
}



dbi.catalog_disconnect <- function(e) {
  on.exit(rm(list = "./dbi_connection", envir = e))
  try(DBI::dbDisconnect(dbi_connection(e)), silent = TRUE)
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



new_schema <- function(schema_name, catalog) {
  init_schema(new.env(parent = emptyenv()), schema_name, catalog)
}



init_schema <- function(schema, schema_name, catalog) {
  assign_and_lock(schema_name, schema, catalog)
  assign_and_lock("./schema_name", schema_name, schema)
  assign_and_lock("../catalog", catalog, schema)
  class(schema) <- "dbi.schema"
  schema
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
