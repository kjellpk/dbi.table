#' Example Databases
#'
#' @description These functions return connections to example databases
#'              included in the \pkg{dbi.table} package.
#'
#' @describeIn example_databases
#'
#' @export
chinook.sqlite <- function() {
  path <- file.path(system.file(package = "dbi.table"),
                    "example_files",
                    "chinook_sqlite.sqlite")

  if (!dir.create(tmp_path <- tempfile("ex"))) {
    stop("could not create directory ", tmp_path)
  }

  tmp_path <- file.path(tmp_path, "chinook_sqlite.sqlite")

  if (!file.copy(path, tmp_path, overwrite = FALSE, copy.mode = FALSE)) {
    stop("could not copy 'chinook_sqlite.sqlite' to temporary directory")
  }

  #' @importFrom DBI dbConnect
  dbConnect(RSQLite::SQLite(), tmp_path)
}



#' @describeIn example_databases
#'
#' @export
chinook.duckdb <- function() {
  path <- file.path(system.file(package = "dbi.table"),
                    "example_files",
                    "chinook_parquet")

  if (!dir.create(tmp_path <- tempfile("ex"))) {
    stop("could not create directory ", tmp_path)
  }

  tmp_path <- file.path(tmp_path, "chinook_duckdb.duckdb")

  #' @importFrom DBI dbConnect
  conn <- dbConnect(duckdb::duckdb(), tmp_path)

  #' @importFrom DBI dbExecute
  status <- dbExecute(conn, paste0("IMPORT DATABASE '", path, "';"))

  conn
}
