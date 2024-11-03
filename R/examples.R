#' @name example_databases
#'
#' @title Example Databases
#'
#' @description These zero-argument functions return connections to the example
#'              databases included in the \pkg{dbi.table} package.
#'
#' @export
chinook.sqlite <- function() {
  if (!requireNamespace("RSQLite", quietly = TRUE)) {
    stop("the \"RSQLite\" package must be installed to use 'chinook.sqlite'",
         call. = FALSE)
  }

  conn <- DBI::dbConnect(RSQLite::SQLite(),
                         temp_db_path("chinook_sqlite.sqlite"))

  load_chinook_database(conn)
}



#' @rdname example_databases
#'
#' @export
chinook.duckdb <- function() {
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("the \"duckdb\" package must be installed to use 'chinook.duckdb'",
         call. = FALSE)
  }

  conn <- DBI::dbConnect(duckdb::duckdb(),
                         temp_db_path("chinook_duckdb.duckdb"))

  load_chinook_database(conn)
}



temp_db_path <- function(db_file_name) {
  if (!dir.create(tmp_path <- tempfile("ex"))) {
    stop("could not create directory ", tmp_path)
  }

  file.path(tmp_path, db_file_name)
}



load_chinook_database <- function(conn) {
  if (length(DBI::dbListTables(conn))) {
    stop("database is not empty", call. = FALSE)
  }

  chinook_dir <- file.path(system.file(package = "dbi.table"),
                           "example_files",
                           "chinook_export")

  chinook_schema <- file.path(chinook_dir, "schema.sql")

  chinook_sql <- readLines(chinook_schema)

  for (statement in chinook_sql[nchar(chinook_sql) > 0L]) {
    DBI::dbExecute(conn, statement)
  }

  chinook_tables <- c("Artist", "Employee", "Genre", "MediaType", "Playlist",
                      "Album", "Customer", "Invoice", "Track", "InvoiceLine",
                      "PlaylistTrack")

  for (tab in chinook_tables) {
    x <- fread(file.path(chinook_dir, paste0(tolower(tab), ".csv.bz2")))
    DBI::dbAppendTable(conn, tab, x)
  }

  conn
}