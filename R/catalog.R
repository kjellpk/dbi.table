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
    data.frame(field = DBI::dbListFields(conn, id))
  }

  list(ids = ids, fields_fun = fields_fun)
}



catalog_primary_keys <- function(catalog, ...) {
  UseMethod("catalog_primary_keys", dbi_connection(catalog))
}



#' @rawNamespace S3method(catalog_primary_keys,default,catalog_primary_keys_dbi)
catalog_primary_keys_dbi <- function(catalog, ...) {
  NULL
}



catalog_foreign_keys <- function(catalog, ...) {
  UseMethod("catalog_foreign_keys", dbi_connection(catalog))
}



#' @rawNamespace S3method(catalog_foreign_keys,default,catalog_foreign_keys_dbi)
catalog_foreign_keys_dbi <- function(catalog, ...) {
  NULL
}



catalog_dbi_table_templates <- function(catalog, ...) {
  use_DBI <- isTRUE(list(...)$DBI)

  objs <- if (use_DBI) {
    catalog_objects_dbi(catalog, ...)
  } else {
    catalog_objects(catalog, ...)
  }

  templates <- if (is.data.frame(objs)) {
    id_cols <- intersect(c("catalog", "schema", "table"), names(objs))
    objs$id <- apply(objs[, id_cols, drop = FALSE], 1L, DBI::Id)
    ff <- mapply(DBI::dbQuoteIdentifier,
                 x = objs$id, MoreArgs = list(conn = catalog),
                 USE.NAMES = FALSE)
    objs <- split(objs, ff)

    ids <- lapply(objs, function(u) u[[1L, "id"]])
    fields <- lapply(objs, function(u) {
      u[u$field_index, c("field", "type")]
    })

    mapply(dbi_table_template,
           id = ids, fields = fields,
           MoreArgs = list(catalog = catalog))
  } else {
    lapply(objs$ids, dbi_table_template,
           catalog = catalog, fields = objs$fields_fun)
  }

  assign("./dbi_table_templates", templates, catalog)

  pks <- if (use_DBI) {
    catalog_primary_keys_dbi(catalog, ...)
  } else {
    catalog_primary_keys(catalog, ...)
  }

  if (is.data.frame(pks)) {
    id_cols <- intersect(c("catalog", "schema", "table"), names(pks))
    pks$id <- apply(pks[, id_cols, drop = FALSE], 1L, DBI::Id)
    ff <- mapply(DBI::dbQuoteIdentifier,
                 x = pks$id, MoreArgs = list(conn = catalog),
                 USE.NAMES = FALSE)
    pks <- split(pks, ff)
    pks <- lapply(pks, function(u) u[u$column_index, "column"])
    lapply(names(pks), function(u) {
      assign(".primary_key", pks[[u]], templates[[u]])
    })
  }

  if (is.function(pks)) {
    mapply(assign, pos = templates,
           MoreArgs = list(x = ".primary_key", value = pks))
  }

  fks <- if (use_DBI) {
    catalog_foreign_keys_dbi(catalog, ...)
  } else {
    catalog_foreign_keys(catalog, ...)
  }

  if (is.data.frame(fks)) {
    ids <- id_names_from_df(fks, catalog)
    fks <- lapply(split(fks, ids), fk_df_to_list, conn = catalog)

    lapply(names(fks), function(u) {
      assign(".foreign_keys", fks[[u]], templates[[u]])
    })
  }

  if (is.function(fks)) {
    mapply(assign, pos = templates,
           MoreArgs = list(x = ".foreign_keys", value = fks))
  }

  templates
}



fk_df_to_list <- function(fk_df, conn) {
  fk_df$ref_ids <- id_names_from_df(fk_df, conn, ref = TRUE)

  lapply(split(fk_df, f = fk_df$foreign_key_name), function(u) {
    list(referenced_id = u[[1L, "ref_ids"]],
         foreign_key = u$column_name,
         primary_key = u$ref_column_name)
  })
}



ID_COLS <- c("table_catalog", "table_schema", "table_name")
REF_ID_COLS <- paste("ref", ID_COLS, sep = "_")

id_names_from_df <- function(x, conn, ref = FALSE) {
  id_cols <- intersect(if (ref) REF_ID_COLS else ID_COLS, names(x))
  mapply(DBI::dbQuoteIdentifier,
         x = apply(x[, id_cols, drop = FALSE], 1L, DBI::Id),
         MoreArgs = list(conn = conn),
         USE.NAMES = FALSE)
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
