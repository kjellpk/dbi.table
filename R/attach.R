#' Attach a Database Schema to the Search Path
#'
#' @description
#'   Create a \code{\link{dbi.table}} for each database object in a schema and
#'   place them on the search path.
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

  db <- dbi.catalog(what)

  schemas <- setdiff(ls(db), c("information_schema", "pg_catalog"))

  if (is.null(schema)) {
    if (length(schemas) == 1L) {
      schema <- schemas[[1L]]
    } else if (length(schemas) && interactive()) {
      schema <- utils::menu(schemas,
                            graphics = graphics,
                            title = "Select Schema")
      if (schema > 0) {
        schema <- schemas[[schema]]
      } else {
        return(invisible())
      }
    } else {
      stop("error setting up database")
    }
  } else {
    if (!(schema %chin% schemas)) {
      stop("schema '", schema, "' not found")
    }
  }

  if (is.null(name)) {
    if (schema %chin% c("main", "dbo")) {
      name <- db_short_name(what)
    } else {
      name <- schema
    }

    name <- paste(dbi_connection_package(what), name, sep = ":")
  } else {
    name <- paste(dbi_connection_package(what),
                  as.character(name)[[1L]],
                  sep = ":")
  }

  if (name %in% search()) {
    stop("'", what_name, "' was not attached because '", name,
         "' is already on the search path - if you want to attach the same ",
         "database twice, use the 'name' argument to provide a distinct name")
  }

  # From ?attach: "In programming, functions should not change the search
  #                path unless that is their purpose."
  #
  # The intended purpose of dbi.attach is to add data on the search path.

  e <- get("attach", "package:base")(NULL, pos = pos, name = name,
           warn.conflicts = warn.conflicts)

  e <- init_schema(e, schema, db)

  for (tab in ls(db[[schema]])) {
    e[[tab]] <- db[[schema]][[tab]]
    lockBinding(tab, e)
  }

  rm(list = schema, pos = db)
  db[[schema]] <- e
  lockBinding(schema, db)

  invisible(e)
}
