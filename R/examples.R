#' Example Databases
#'
#' @description These functions return connections to example databases
#'              included in the \pkg{dbi.table} package.
#'
#' @describeIn example_databases
#'
#' @export
ex_chinook <- function() {
  if (requireNamespace("RSQLite")) {
    path <- system.file(package = "dbi.table")
    path <- file.path(path, "example_files", "Chinook_Sqlite.sqlite")
    DBI::dbConnect(RSQLite::SQLite(), path)
  } else {
    stop("the ", dQuote("RSQLite"), " package is required to use this example")
  }
}
