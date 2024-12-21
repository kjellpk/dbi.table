#' Create a \code{dbi.catalog}
#'
#' A \code{dbi.catalog} represents a database catalog.
#'
#' @param conn
#'   a connection handle returned by \code{\link[DBI]{dbConnect}} or a
#'   zero-argument function that returns a connection handle.
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
dbi.catalog <- function(conn, schemas = NULL) {
  conn <- init_connection(conn)
  new_dbi_catalog(conn, schemas, NULL)
}



new_dbi_catalog <- function(conn, schemas, columns) {
  catalog <- new.env(parent = emptyenv())
  assign("./dbi_connection", conn, catalog)

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(catalog, dbi.catalog_disconnect, onexit = TRUE)
  }

  class(catalog) <- "dbi.catalog"

  if (is.null(columns <- get_init_columns(catalog))) {
    info <- bare_bones_information_schema(catalog)
    columns <- copy(info$columns)
    setkeyv(columns, c("table_name", "ordinal_position"))
  } else {
    info <- information_schema(catalog, columns)
  }

  if (is.null(columns$table_schema)) {
    schema_names <- "main"
  } else {
    schema_names <- setdiff(unique(columns$table_schema), "information.schema")
  }

  if (is.null(schemas)) {
    schemas <- schema_names
  } else if (!is.list(schemas)) {
    schemas <- as.character(schemas)
    schemas <- intersect(schemas, schema_names)
  } else {
    schemas <- schemas[intersect(names(schemas), schema_names)]
  }

  if (is.character(schemas)) {
    names(schemas) <- schemas
    schemas <- lapply(schemas, new_schema, catalog = catalog)
  }

  install_from_columns(columns, schemas, catalog)

  catalog
}



install_from_columns <- function(columns, schemas, catalog) {
  schema_names <- names(schemas)
  by_cols <- intersect(c("table_catalog", "table_schema", "table_name"),
                       names(columns))

  tables <- columns[, .(dbi_table = list(new_dbi_table(catalog,
                                                       DBI::Id(unlist(.BY)),
                                                       column_name))),
                    by = by_cols]
  if (is.null(tables$table_schema)) {
    tables[, table_schema := schema_names[[1L]]]
  } else {
    tables <- tables[table_schema %chin% schema_names]
  }

  tables[, schema := schemas[table_schema]]
  dev_null <- mapply(assign_and_lock,
                     x = tables$table_name,
                     value = tables$dbi_table,
                     pos = tables$schema,
                     SIMPLIFY = FALSE,
                     USE.NAMES = FALSE)

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



new_schema <- function(schema_name, catalog) {
  init_schema(new.env(parent = emptyenv()), schema_name, catalog)
}



init_schema <- function(schema, schema_name, catalog) {
  assign(schema_name, schema, catalog)
  lockBinding(schema_name, catalog)

  assign("./schema_name", schema_name, schema)
  lockBinding("./schema_name", schema)

  assign("../catalog", catalog, schema)
  lockBinding("../catalog", schema)

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
