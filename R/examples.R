#' Example Databases
#'
#' @description These functions return connections to example databases
#'              included in the \pkg{dbi.table} package.
#'
#' @describeIn example_databases
#'
#' @export
chinook.sqlite <- function() {
  Chinook_Sqlite.sqlite <- "Chinook_Sqlite.sqlite"
  path <- system.file(package = "dbi.table")
  path <- file.path(path, "example_files", Chinook_Sqlite.sqlite)

  if (!dir.create(tmp_path <- tempfile("ex"))) {
    stop("could not create directory ", tmp_path)
  }

  tmp_path <- file.path(tmp_path, Chinook_Sqlite.sqlite)

  if (!file.copy(path, tmp_path, overwrite = FALSE, copy.mode = FALSE)) {
    stop("could not copy 'Chinook_Sqlite.sqlite' to temporary directory")
  }

  if (requireNamespace("RSQLite")) {
    return(DBI::dbConnect(RSQLite::SQLite(), tmp_path))
  }

  stop("the ", dQuote("RSQLite"), " package is required to use this example")
}
