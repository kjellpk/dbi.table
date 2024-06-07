#' Example Databases
#'
#' @description These functions return connections to example databases
#'              included in the \pkg{dbi.table} package.
#'
#' @describeIn example_databases
#'
#' @export
chinook.sqlite <- function() {
  if (!requireNamespace("RSQLite", quietly = TRUE)) {
    stop("the \"RSQLite\" package must be installed to use 'chinook.sqlite'",
         call. = FALSE)
  }

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

  DBI::dbConnect(RSQLite::SQLite(), tmp_path)
}



#' @describeIn example_databases
#'
#' @export
chinook.duckdb <- function() {
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("the \"duckdb\" package must be installed to use 'chinook.duckdb'",
         call. = FALSE)
  }


  path <- file.path(system.file(package = "dbi.table"),
                    "example_files",
                    "chinook_parquet")

  if (!dir.create(tmp_path <- tempfile("ex"))) {
    stop("could not create directory ", tmp_path)
  }

  tmp_path <- file.path(tmp_path, "chinook_duckdb.duckdb")

  conn <- DBI::dbConnect(duckdb::duckdb(), tmp_path)

  status <- DBI::dbExecute(conn, paste0("IMPORT DATABASE '", path, "';"))

  conn
}
