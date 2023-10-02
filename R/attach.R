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
#' @param \dots additional arguments are passed to
#'              \code{\link[DBI]{dbListObjects}}.
#'
#' @param prefix see the \code{prefix} argument in
#'               \code{\link[DBI]{dbListObjects}}.
#'
#' @note Attaches each object returned by \code{\link[DBI]{dbListObjects}}.
#'
#' @seealso \code{\link[base]{attach}}
#'
#' @export
dbi.attach <- function(what, pos = 2L, name = NULL, warn.conflicts = FALSE,
                       ..., prefix = NULL) {
  recon <- NULL
  if (is.function(what)) {
    recon <- what
    what <- what()
  }

  if (!isa(what, "DBIConnection")) {
    stop(sQuote("what"), " is not a ", sQuote("DBIConnection"))
  }

  if (is.null(name)) {
    name <- db_short_name(what, pkg = TRUE)
  }

  #' @importFrom DBI dbListObjects
  schema <- dbListObjects(what, prefix = prefix, ...)
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

  assign(".dbi", what, envir = e)
  assign(".recon", recon, envir = e)

  reg.finalizer(e, dbi_attach_finalizer, onexit = TRUE)

  for (i in seq_len(nrow(schema))) {
    dbit_name <- schema[[i, "id"]]@name[["table"]]
    dbit <- new_dbi_table(e, schema[[i, "id"]], schema[[i, "column_names"]])

    if (!is.na(dbit_name)) {
      assign(dbit_name, dbit, envir = e)
      lockBinding(dbit_name, e)
    }
  }

  invisible(e)
}



dbi_attach_finalizer <- function(e) {
  e$.recon <- NULL
  #' @importFrom DBI dbDisconnect
  dbDisconnect(e$.dbi)
}
