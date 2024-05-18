#' Example Databases
#'
#' @description These functions return connections to example databases
#'              included in the \pkg{dbi.table} package.
#'
#' @describeIn example_databases
#'
#' @export
chinook.sqlite <- function() {
  example_connection("chinook_sqlite.sqlite", RSQLite::SQLite)
}



#' @describeIn example_databases
#'
#' @export
chinook.duckdb <- function() {
  example_connection("chinook_duckdb.duckdb", duckdb::duckdb)
}



example_connection <- function(db, drv) {
  path <- system.file(package = "dbi.table")
  path <- file.path(path, "example_files", db)

  if (!dir.create(tmp_path <- tempfile("ex"))) {
    stop("could not create directory ", tmp_path)
  }

  tmp_path <- file.path(tmp_path, db)

  if (!file.copy(path, tmp_path, overwrite = FALSE, copy.mode = FALSE)) {
    stop("could not copy '", db, "' to temporary directory")
  }

  #' @importFrom DBI dbConnect
  dbConnect(drv(), tmp_path)
}
