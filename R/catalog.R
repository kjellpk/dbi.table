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

  columns <- tables_schema(dbi_connection(catalog))
  columns$ordinal_position <- as.integer(columns$ordinal_position)
  columns$pk_ordinal_position <- as.integer(columns$pk_ordinal_position)

  if (is.null(columns$table_schema)) {
    schema_names <- columns$table_schema <- "main"
  } else {
    schema_names <- unique(columns$table_schema)
  }

  if (is.na(schemas)) {
    schemas <- schema_names
  } else {
    if (length(not_found <- setdiff(schemas, schema_names))) {
      stop("schemas not found on connection: ",
           paste(not_found, collapse = ", "))
    }

    schemas <- union(schemas, schemas_to_include(conn))
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
    key <- subset(u, subset = !is.na(u$pk_ordinal_position))
    key <- key$column_name[key$pk_ordinal_position]

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

    install_in_schema(table_name, catalog, id, fields,
                      column_names, key, schema)
  })

  invisible()
}



dbi.catalog_disconnect <- function(e) {
  on.exit(rm(list = "./dbi_connection", envir = e))
  stry(DBI::dbDisconnect(dbi_connection(e)))
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



schemas_to_include <- function(conn) {
  UseMethod("schemas_to_include")
}



#' @rawNamespace S3method(schemas_to_include,default,schemas_to_include_default)
schemas_to_include_default <- function(conn) {
  character(0)
}



#' @rawNamespace S3method(schemas_to_include,"Microsoft SQL Server",schemas_to_include_Microsoft_SQL_Server)
schemas_to_include_Microsoft_SQL_Server <- function(conn) {
  c("INFORMATION_SCHEMA", "sys")
}



#' @rawNamespace S3method(schemas_to_include,MariaDBConnection,schemas_to_include_mariadb)
schemas_to_include_mariadb <- function(conn) {
  c("information_schema")
}


#' @rawNamespace S3method(schemas_to_include,PqConnection,schemas_to_include_postgres)
schemas_to_include_postgres <- function(conn) {
  c("information_schema", "pg_catalog")
}



#' @rawNamespace S3method(schemas_to_include,duckdb_connection,schemas_to_include_duckdb)
schemas_to_include_duckdb <- function(conn) {
  c("information_schema")
}
