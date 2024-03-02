#' Attach a Database Schema to the Search Path
#'
#' @description Create a \code{\link{dbi.table}} for each database object in
#'              a schema and place them on the search path.
#'
#' @param what a connection handle returned by \code{\link[DBI]{dbConnect}} or
#'             a zero-argument function that returns a connection handle.
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
#' @seealso \code{\link[base]{attach}}
#'
#' @export
dbi.attach <- function(what, pos = 2L, name = NULL, warn.conflicts = FALSE) {
  what_name <- deparse1(substitute(what))
  what <- init_connection(what)

  if (is.null(name)) {
    name <- db_short_name(what)
  }

  name <- paste(dbi_connection_package(what), as.character(name[1]), sep = ":")

  if (name %in% search()) {
    stop(sQuote(what_name), " was not attached because ", sQuote(name),
         " is already on the search path - use the ", sQuote("name"),
         " argument to provide a distinct name")
  }

  # From ?attach: "In programming, functions should not change the search
  #                path unless that is their purpose."
  #
  # The intended purpose of dbi.attach is to add data on the search path.
  # Mask the attach funcation as 'fun' to avoid R CMD check error.

  e <- get("attach", "package:base")(NULL, pos = pos, name = name,
           warn.conflicts = warn.conflicts)

  initialize_dbi_database(what, e)
  add_db_objects(e, db_objects(e))

  invisible(e)
}
