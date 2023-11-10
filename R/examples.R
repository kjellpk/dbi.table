#' Example Databases
#'
#' @description These functions return connections to example databases
#'              included in the \pkg{dbi.table} package.
#'
#' @param type a scalar character string. Use \code{"DBI"} for a
#'             \code{DBIConnection} or \code{path} for the database file path.
#'
#' @describeIn example_databases
#'
#' @export
chinook.sqlite <- function(type = c("DBI", "path")) {
  type <- match.arg(type)

  path <- system.file(package = "dbi.table")
  path <- file.path(path, "example_files", "Chinook_Sqlite.sqlite")

  if (type == "path") {
    return(path)
  }

  if (requireNamespace("RSQLite")) {
    DBI::dbConnect(RSQLite::SQLite(), path)
  } else {
    stop("the ", dQuote("RSQLite"), " package is required to use this example")
  }
}
