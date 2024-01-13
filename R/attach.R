#' Attach a DBI Connection to the Search Path
#'
#' @description Attach all (or a subset) of the database objects (tables, views,
#'              etc.) accessible over a DBI connection to the search path. The
#'              attached objects can be queried using
#'              \code{\link[data.table]{data.table}} syntax.
#'
#' @param what a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'
#' @param pos an integer specifying position in \code{\link[base]{search}}()
#'            where to attach.
#'
#' @param name a character string specifying the name to use for the attached
#'             database.
#'
#' @param warn.conflicts a logical value. If \code{TRUE}, warnings are
#'                       printed about \code{\link[base]{conflicts}} from
#'                       attaching the database, unless that database contains
#'                       an object \code{.conflicts.OK}. A conflict is a
#'                       function masking a function, or a non-function
#'                       masking a non-function.
#'
#' @param \dots additional arguments (e.g., \code{prefix}) are passed to
#'              \code{\link[DBI]{dbListObjects}}.
#'
#' @note Attaches each object returned by \code{\link[DBI]{dbListObjects}}.
#'
#' @seealso \code{\link[base]{attach}}
#'
#' @export
dbi.attach <- function(what, pos = 2L, name = NULL, warn.conflicts = FALSE,
                       ...) {
  what_name <- deparse1(substitute(what))
  what <- init_connection(what)
  check_connection(what, arg_name = "what")

  if (is.null(name)) {
    name <- db_short_name(what)
  }

  name <- paste(dbi_connection_package(what), as.character(name[1]), sep = ":")

  if (name %in% search()) {
    stop(sQuote(what_name), " was not attached because ", sQuote(name),
         " is already on the search path - use the ", sQuote("name"),
         " argument to provide a distinct name")
  }

  #' @importFrom DBI dbListObjects
  schema <- dbListObjects(what, ...)
  schema <- schema[!schema$is_prefix, "table"]
  names(schema) <- vapply(schema, function(u) u@name[["table"]], "")
  #' @importFrom DBI dbListFields
  fields <- mapply(dbListFields, schema, MoreArgs = list(conn = what))
  schema <- list(id = schema, fields = fields)

  # From ?attach: "In programming, functions should not change the search
  #                path unless that is their purpose."
  #
  # The intended purpose of dbi.attach is to change the search path. The call
  # to attach is masked here to avoid the unsuppressible R CMD check Note.

  fun <- get("attach", "package:base")
  e <- fun(NULL, pos = pos, name = name, warn.conflicts = warn.conflicts)

  schema_env(what, schema, e)
}



schema_env_finalizer <- function(e) {
  on.exit(rm(list = ".dbi_connection", envir = e))
  #' @importFrom DBI dbDisconnect
  try(dbDisconnect(dbi_connection(e)), silent = TRUE)
}



schema_env <- function(conn, schema, envir = new.env(parent = emptyenv())) {
  if (!is.null(existing_conn <- envir$.dbi_connection)) {
    if (!identical(conn, existing_conn)) {
      stop("multiple connections are not supported in a single schema")
    }
  } else {
    envir$.dbi_connection <- conn
  }

  if (anyDuplicated(dbit_names <- names(schema$id))) {
    stop("duplicate names found in ", sQuote("schema"))
  }

  for (dbit_name in dbit_names) {
    dbit <- new_dbi_table(envir,
                          schema$id[[dbit_name]],
                          schema$fields[[dbit_name]])

    assign(dbit_name, dbit, envir = envir)
    lockBinding(dbit_name, envir)
  }

  reg.finalizer(envir, schema_env_finalizer, onexit = TRUE)

  invisible(envir)
}
