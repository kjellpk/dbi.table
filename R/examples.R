#' Example Databases
#'
#' @description These functions return connections to example databases
#'              included in the \pkg{dbi.table} package.
#'
#'
#' @param type a character string. Use \code{"pool"} or \code{"DBI"}.
#'
#' @describeIn example_databases
#'
#' @export
chinook.sqlite <- function(type = c("Pool", "DBI")) {
  type <- match.arg(type)

  if (requireNamespace("RSQLite")) {
    path <- system.file(package = "dbi.table")
    path <- file.path(path, "example_files", "Chinook_Sqlite.sqlite")

    if (type == "Pool") {
      pool::dbPool(RSQLite::SQLite(), dbname = path)
    } else {
      DBI::dbConnect(RSQLite::SQLite(), path)
    }
  } else {
    stop("the ", dQuote("RSQLite"), " package is required to use this example")
  }
}
