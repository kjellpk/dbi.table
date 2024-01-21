#' Create a \code{dbi_database}
#'
#' Use \code{dbi_database} to create a \code{dbi_database},
#' \code{list_db_objects} to get a list of database objects (e.g., tables and
#' views) that can be added to the \code{dbi_database}, and
#' \code{add_db_objects} to add one or more of these objects. See Details and
#' Examples.
#'
#' A \code{dbi_database} is an \code{environment} with the class
#' attribute set to \code{"dbi_database"}. Initially, the \code{dbi_database}
#' contains only the \code{\link{information_schema}}. When a database object
#' (e.g., a table or view identified by an \code{\link[DBI]{Id}}) is
#' added to the \code{dbi_database} using \code{add_db_objects}, a
#' \code{\link{dbi.table}} is created in the \code{dbi_database}. Since the
#' \code{dbi_database} is an envionment, the added \code{\link{dbi.table}} can
#' be accessed using the \code{$} operator. Further, one can use
#' \code{\link[base]{ls}} and \code{\link[base]{objects}} to list the
#' \code{\link{dbi.table}}s in the \code{dbi_database}. See Examples.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}} or
#'             a zero-argument function that returns a connection handle.
#'
#'
#' @return \code{dbi_database} returns a \code{dbi_database} (internally an
#'         \code{\link[base]{environment}} with the class attribute set to
#'         \code{"dbi_database"}).
#'
#'         \code{list_db_objects} returns a named list of
#'         \code{\link[DBI]{Id}}s.
#'
#'         \code{add_db_objects} invisibly returns \code{db}.
#'
#' @examples
#' # chinook.sqlite is a zero-argument function that returns a DBI handle
#' chinook_db <- dbi_database(chinook.sqlite)
#'
#' # Initially, only the information.schema is included
#' ls(chinook_db, all = TRUE)
#'
#' # List the available database objects
#' (chinook_objects <- list_db_objects(chinook_db))
#'
#' # Add the Playlist table to chinook_db, name it Boss_Playlist
#' add_db_objects(chinook_db, Boss_Playlist = DBI::Id(table = "Playlist"))
#'
#' ls(chinook_db)
#'
#' # Preview the dbi.table
#' chinook_db$Boss_Playlist
#'
#' # Add the Artist and Album tables to chinook_db
#' (ArtistAndAlbum <- chinook_objects[c("Artist", "Album")])
#' add_db_objects(chinook_db, ArtistAndAlbum)
#'
#' # chinook_db now contains 3 dbi.tables
#' ls(chinook_db)
#'
#' @export
dbi_database <- function(conn) {
  conn <- init_connection(conn)
  info_s <- information_schema(conn)
  db <- new.env(parent = emptyenv())
  assign(".information_schema", info_s, pos = db)
  class(db) <- "dbi_database"
  db
}



#' @describeIn dbi_database
#'
#' list the database objects in \code{db} that are available to add.
#'
#' @param USE.FULLY.QUALIFIED.NAMES a logical value. When \code{TRUE}, the names
#'                                  of the returned list have the form:
#'                                  \code{catalog.schema.table}. When
#'                                  \code{FALSE}, \code{catalog} and
#'                                  \code{schema} are only included when needed
#'                                  to make the names distinct.
#'
#' @export
list_db_objects <- function(db, USE.FULLY.QUALIFIED.NAMES = FALSE) {
  if (!inherits(db, "dbi_database")) {
    stop(sQuote(deparse1(substitute(db))), " is not a ", sQuote("dbi_database"))
  }

  info_s <- db[[".information_schema"]]
  objs <- list_database_objects(info_s)
  default_names <- default_object_names(objs, USE.FULLY.QUALIFIED.NAMES)
  objs <- objs$table_id
  names(objs) <- default_names

  objs
}



#' @describeIn dbi_database
#'
#' add database objects to \code{db}.
#'
#' @param db a \code{dbi_database} created by \code{\link{dbi_database}}.
#'
#' @param \dots a comma-separated list of named \code{\link[DBI]{Id}}s. See
#'              Examples. Alternatively, if \dots contains a single element and
#'              that element is a named list of \code{\link[DBI]{Id}}s
#'              (for example, the list returned by \code{list_db_objects}),
#'              then that list of \code{\link[DBI]{Id}}s will be added.
#'
#' @export
add_db_objects <- function(db, ...) {
  dots <- list(...)

  d1 <- dots[[1]]
  if (is.list(d1) && all(vapply(d1, inherits, TRUE, what = "Id"))) {
    dots <- d1
  }

  dn <- names(dots)

  if (is.null(dn) || any(nchar(dn) == 0L) || anyDuplicated(dn)) {
    stop("all elements must be named and each name must have at least ",
         "one character; duplicate names are not allowed")
  }

  info_s <- db[[".information_schema"]]
  objs <- list_database_objects(info_s)

  if (all(check <- (dots %in% objs$table_id))) {
    for (i in seq_along(dots)) {
      idx <- match(dots[i], objs$table_id)
      dbit <- new_dbi_table(conn = info_s,
                            id = objs[[idx, "table_id"]],
                            fields = objs[[idx, "column_names"]])
      assign(names(dots)[i], dbit, pos = db)
    }
  } else {
    missing_id <- dQuote(dots[[which(!check)[1]]]@name)
    stop(sQuote(paste("<Id>", paste(missing_id, collapse = "."))),
         " not found")
  }

  invisible(db)
}



mini_split_id <- function(by, conn) {
  by <- unlist(by)
  stopifnot(is.character(by) && (length(by) == 3L))
  if (is.na(by[1])) by <- by[-1]
  if (is.na(by[1])) by <- by[-1]
  if (is.na(by[1])) stop("missing ", sQuote("TABLE_NAME"))
  #' @importFrom DBI Id
  #' @importFrom DBI dbQuoteIdentifier
  by <- dbQuoteIdentifier(conn, Id(by))
  #' @importFrom DBI dbUnquoteIdentifier
  dbUnquoteIdentifier(conn, by)
}



list_database_objects <- function(info_s) {
  conn <- info_s[[".dbi_connection"]]
  columns <- info_s$COLUMNS
  columns[, list(table_id = mini_split_id(.BY, conn),
                 column_names = list(COLUMN_NAME)),
          by = list(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME)]
}



default_object_names <- function(objs, USE.FULLY.QUALIFIED.NAMES = FALSE) {
  obj_names <- objs$TABLE_NAME
  if (anyDuplicated(obj_names) || USE.FULLY.QUALIFIED.NAMES) {
    obj_names <- paste(objs$TABLE_SCHEMA, obj_names, sep = ".")
  }
  if (anyDuplicated(obj_names) || USE.FULLY.QUALIFIED.NAMES) {
    obj_names <- paste(objs$TABLE_CATALOG, obj_names, sep = ".")
  }

  obj_names
}
