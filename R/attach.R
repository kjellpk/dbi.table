#' Attach a Database Schema to the Search Path
#'
#' @description
#'   The database schema is attached to the R search path. This means that the
#'   schema is searched by R when evaluating a variable, so that
#'   \code{\link{dbi.table}}s in the schema can be accessed by simply giving
#'   their names.
#'
#' @param what
#'   a connection handle returned by \code{\link[DBI]{dbConnect}} or a
#'   zero-argument function that returns a connection handle.
#'
#' @param pos
#'   an integer specifying position in \code{\link[base]{search}}() where to
#'   attach.
#'
#' @param name
#'   a character string specifying the name to use for the attached database.
#'
#' @param warn.conflicts
#'   a logical value. If \code{TRUE}, warnings are printed about
#'   \code{\link[base]{conflicts}} from attaching the database, unless that
#'   database contains an object \code{.conflicts.OK}. A conflict is a function
#'   masking a function, or a non-function masking a non-function.
#'
#' @param schema
#'   a character string specifying the name of the schema to attach.
#'
#' @param graphics
#'   a logical value; passed to \code{\link[utils]{menu}}. In interactive
#'   sessions, when \code{schema} is \code{NULL} and multiple schemas are
#'   found on \code{what}, a menu is displayed to select a schema.
#'
#' @seealso \code{\link[base]{attach}}
#'
#' @return
#'   an \code{\link{environment}}, the attached schema is invisibly returned.
#'
#' @export
dbi.attach <- function(what, pos = 2L, name = NULL, warn.conflicts = FALSE,
                       schema = NULL, graphics = TRUE) {
  what_name <- deparse1(substitute(what))
  what <- init_connection(what)

  if (length(name) && length(name <- as.character(name)) != 1L) {
    stop("'name' is not a scalar character string")
  }

  if (is.null(schema)) {
    schema <- default_schema(what)
  } else {
    schema <- as.character(schema)

    if (length(schema) != 1L) {
      stop("'schema' is not a scalar character string")
    }
  }

  catalog <- dbi.catalog(what, schemas = schema)
  schemas <- ls(catalog)

  if (is.na(schema)) {
    if (length(schemas) && interactive()) {
      schema <- utils::menu(schemas,
                            graphics = graphics,
                            title = "Select Schema")
      if (schema == 0L) {
        return(invisible())
      } else {
        schema <- schemas[[schema]]
      }
    }
  }

  if (!(schema %in% schemas)) {
    stop("schema '", schema, "' not found on connection '", what_name, "'")
  }

  what <- catalog[[schema]]
  schema <- get("./schema_name", pos = what, inherits = FALSE)

  if (is.null(name)) {
    if (schema %in% c("main", "dbo")) {
      name <- db_short_name(what)
    } else {
      name <- schema
    }

    name <- paste(dbi_connection_package(what), name, sep = ":")
  }

  if (name %in% search()) {
    stop("'", what_name, "' was not attached because '", name,
         "' is already on the search path - if you really want to attach the ",
         "same database twice, use the 'name' argument to provide a distinct ",
         "name")
  }

  # From ?attach: "In programming, functions should not change the search
  #                path unless that is their purpose."
  #
  # The intended purpose of dbi.attach is to add data on the search path.

  e <- get("attach", "package:base")(what, pos = pos, name = name,
                                     warn.conflicts = warn.conflicts)
  class(e) <- "dbi.schema"

  rm(list = schema, pos = catalog)
  assign_and_lock(schema, e, catalog)

  invisible(e)
}



default_schema <- function(conn) {
  UseMethod("default_schema")
}



#' @rawNamespace S3method(default_schema,default,default_schema_default)
default_schema_default <- function(conn) {
  "main"
}
