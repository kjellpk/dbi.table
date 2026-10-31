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
dbi.catalog <- function(conn, ...) {
  catalog <- empty_catalog(conn)
  templates <- catalog_dbi_table_templates(catalog, ...)

  lapply(templates, function(u, catalog) {
    l <- length(id_name <- u$id@name)
    schema_name <- if (l > 1L) id_name[[l - 1L]] else "main"

    if (is.null(schema <- catalog[[schema_name]])) {
      schema <- new_schema(schema_name, catalog)
    }

    makeActiveBinding(id_name[[l]], u$constructor, schema)
  }, catalog = catalog)

  catalog
}



empty_catalog <- function(conn) {
  conn <- init_connection(conn)

  catalog <- new.env(parent = emptyenv())

  assign("./dbi_connection", conn, catalog)
  assign("./specials", get_specials(conn), catalog)

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(catalog, dbi.catalog_disconnect, onexit = TRUE)
  }

  class(catalog) <- "dbi.catalog"
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



catalog_objects <- function(catalog, ...) {
  UseMethod("catalog_objects", dbi_connection(catalog))
}


#' @rawNamespace S3method(catalog_objects,default,catalog_objects_dbi)
catalog_objects_dbi <- function(catalog, ...) {
  db_objs <- DBI::dbListObjects(catalog, ...)
  ids <- db_objs[db_objs$is_prefix == FALSE, "table"]
  names(ids) <- vapply(ids, id_name, "", conn = catalog)

  fields_fun <- function(conn, id) {
    data.frame(fields = DBI::dbListFields(conn, id))
  }

  list(ids = ids, fields_fun = fields_fun)
}



catalog_dbi_table_templates <- function(catalog, ...) {
  objs <- catalog_objects(catalog, ...)

  if (is.data.frame(objs)) {
    tmp <- NULL
  } else {
    templates <- lapply(objs$ids, dbi_table_template,
                        catalog = catalog, fields = objs$fields_fun)

    lapply(templates, function(u, expr) {
      assign(".fields_expr", expr, pos = u)
    }, expr = objs$fields_expr)
  }

  templates
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
