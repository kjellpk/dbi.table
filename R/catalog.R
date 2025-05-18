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
#'   into the \code{dbi.catalog}. By default (when \code{schemas} is missing),
#'   \code{dbi.catalog} loads all available schemas.
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
dbi.catalog <- function(conn, schemas) {
  conn <- init_connection(conn)

  if (missing(schemas)) {
    schemas <- NA
  }

  schemas <- as.character(schemas)
  catalog <- new.env(parent = emptyenv())
  assign("./dbi_connection", conn, catalog)

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(catalog, dbi.catalog_disconnect, onexit = TRUE)
  }

  class(catalog) <- "dbi.catalog"
  tables_schema <- tables_schema(catalog)

  if (is.na(schemas)) {
    schemas <- unique(tables_schema$table_schema)
  } else {
    if (length(not_found <- setdiff(schemas, tables_schema$table_schema))) {
      stop("schemas not found on connection: ",
           paste(not_found, collapse = ", "))
    }

    schemas <- union(schemas, schemas_to_include(conn))
  }

  tables_schema <- tables_schema[tables_schema$table_schema %in% schemas, ]
  row.names(tables_schema) <- NULL

  names(schemas) <- schemas
  schemas <- lapply(schemas, new_schema, catalog = catalog)
  schemas <- schemas[tables_schema$table_schema]

  table_name <- tables_schema[, "table_name"]
  table_id <- tables_schema$id

  assign("./tables_schema", tables_schema[, c("id", "fields", "key")],
         pos = catalog)

  mapply(install_active_dbi_table,
         schema = schemas,
         name = table_name,
         id = table_id,
         MoreArgs = list(catalog = catalog),
         SIMPLIFY = FALSE,
         USE.NAMES = FALSE)

  catalog
}



dbi.catalog_disconnect <- function(e) {
  on.exit(rm(list = "./dbi_connection", envir = e))
  stry(DBI::dbDisconnect(dbi_connection(e)))
}



#' @export
print.dbi.catalog <- function(x, ...) {
  name <- paste(dbi_connection_package(x), db_short_name(x), sep = "::")
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



schemas_to_include <- function(conn) {
  UseMethod("schemas_to_include")
}



#' @rawNamespace S3method(schemas_to_include,default,schemas_to_include_default)
schemas_to_include_default <- function(conn) {
  character(0)
}
