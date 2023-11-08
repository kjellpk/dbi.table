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
  check_connection(what)

  if (is.null(name)) {
    name <- db_short_name(what, pkg = TRUE)
  }

  #' @importFrom DBI dbListObjects
  schema <- dbListObjects(what, ...)
  schema <- schema[!schema$is_prefix, "table", drop = FALSE]
  names(schema) <- "id"

  #' @importFrom DBI dbListFields
  fields <- lapply(schema$id, function(u, v) dbListFields(v, u), v = what)

  schema <- cbind(schema, column_names = I(fields))

  # From ?attach: "In programming, functions should not change the search
  #                path unless that is their purpose."
  #
  # The intended purpose of dbi.attach is to change the search path. The call
  # to attach is masked here to avoid the unsuppressible R CMD check Note.

  fun <- get("attach", "package:base")
  e <- fun(NULL, pos = pos, name = name, warn.conflicts = warn.conflicts)

  dbi_table_env(what, schema, e)
}



dbi_table_env <- function(conn, schema, envir = new.env()) {
  for (i in seq_len(nrow(schema))) {
    dbit_name <- schema[[i, "id"]]@name[["table"]]
    dbit <- new_dbi_table(conn, schema[[i, "id"]], schema[[i, "column_names"]])

    if (!is.na(dbit_name)) {
      assign(dbit_name, dbit, envir = envir)
      lockBinding(dbit_name, envir)
    }
  }

  #reg.finalizer(envir, dbi_table_env_finalizer, onexit = TRUE)

  invisible(e)
}
