#' Create an Information Schema for a \code{DBIConnection}
#'
#' Caution: this function is not intended for end-users. Unless you are a
#' developer adding a method for an RDBMS that does not provide an Information
#' Schema, you do not need to work with this function directly; it is called by
#' \code{\link{dbi_database}} when initializing a new \code{dbi_database}.
#'
#' An \code{information_schema} is an \code{\link[base]{environment}} that
#' contains
#'  \enumerate{
#'  \item a \code{\link[DBI]{DBIConnection-class}} named \code{.dbi_connection},
#'  \item a \code{\link[data.table]{data.table}} or \code{\link{dbi.table}}
#'        named \code{TABLES}, and
#'  \item a \code{\link[data.table]{data.table}} or \code{\link{dbi.table}}
#'        named \code{COLUMNS}.
#' }
#' The \code{TABLES} \code{*.table} must have columns named
#' \code{TABLE_CATALOG}, \code{TABLE_SCHEMA}, and \code{TABLE_NAME}. If
#' \code{TABLE_CATALOG} and/or \code{TABLE_SCHEMA} are not needed they may be
#' \code{NA_character_}. \code{TABLE_NAME} must be type \code{character} and
#' cannot have any missing values (\code{NA}s).
#'
#' The \code{COLUMNS} \code{*.table} must have columns named
#' \code{TABLE_CATALOG}, \code{TABLE_SCHEMA}, \code{TABLE_NAME},
#' \code{COLUMN_NAME}, and \code{ORDINAL_POSITION}. If
#' \code{TABLE_CATALOG} and/or \code{TABLE_SCHEMA} are not needed they may be
#' \code{NA_character_}. \code{TABLE_NAME} and \code{COLUMN_NAME} must be type
#' \code{character} and cannot have any missing values (\code{NA}s).
#'
#' \code{information_schema} is an S3 generic. The default method uses the
#' RDBMS's Information Schema if it exists (in which case \code{TABLES} and
#' \code{COLUMNS} are \code{\link{dbi.table}}s). Otherwise, it uses
#' \code{\link[DBI]{dbListTables}} and \code{\link[DBI]{dbListFields}} to create
#' a \emph{bare bones} information schema.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'             Note: unlike other functions in the \code{dbi.table} package,
#'             \code{conn} must be the actual
#'             \code{\link[DBI]{DBIConnection-class}} handle returned by
#'             \code{\link[DBI]{dbConnect}} and NOT a function that creates one.
#'
#' @return an \code{\link[base]{environment}} containing the componenets
#'         enumerated in Details.
#'
#' @export
information_schema <- function(conn) {
  UseMethod("information_schema")
}



#' @export
information_schema.default <- function(conn) {
  info_s <- new.env(parent = emptyenv())
  assign(".dbi_connection", conn, pos = info_s)
  bare_bones_information_schema(info_s)
  reg.finalizer(info_s, information_schema_finalizer, onexit = TRUE)
  info_s
}



information_schema_finalizer <- function(e) {
  on.exit(rm(list = ".dbi_connection", envir = e))
  #' @importFrom DBI dbDisconnect
  try(dbDisconnect(e[[".dbi_connection"]]), silent = TRUE)
}



bare_bones_information_schema <- function(info_s) {
  conn <- info_s[[".dbi_connection"]]
  #' @importFrom DBI dbListTables
  TABLES <- data.table(TABLE_CATALOG = NA_character_,
                       TABLE_SCHEMA = NA_character_,
                       TABLE_NAME	= dbListTables(conn),
                       TABLE_TYPE = "BASE TABLE")

  assign("TABLES", TABLES, pos = info_s)

  #' @importFrom DBI dbListFields
  COLUMNS <- mapply(dbListFields, name = TABLES$TABLE_NAME,
                    MoreArgs = list(conn = conn), SIMPLIFY = FALSE)
  COLUMNS <- lapply(COLUMNS, function(u) data.table(COLUMN_NAME = u))
  COLUMNS <- rbindlist(COLUMNS, idcol = "TABLE_NAME")
  COLUMNS[, ORDINAL_POSITION := seq_len(.N), by = list(TABLE_NAME)]
  COLUMNS[, TABLE_CATALOG := NA_character_]
  COLUMNS[, TABLE_SCHEMA := NA_character_]
  setcolorder(COLUMNS, c("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME",
                         "COLUMN_NAME", "ORDINAL_POSITION"))

  assign("COLUMNS", COLUMNS, pos = info_s)

  invisible()
}



# Define globally for R CMD check
TABLE_CATALOG <- NULL
TABLE_NAME <- NULL
TABLE_SCHEMA <- NULL
COLUMN_NAME <- NULL
ORDINAL_POSITION <- NULL
