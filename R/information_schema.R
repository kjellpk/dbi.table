#' Create an Information Schema for a \code{DBIConnection}
#'
#' Caution: this function is not intended for end-users. Unless you are a
#' developer adding a method for an RDBMS that does not provide an Information
#' Schema, you do not need to work with this function directly; it is called by
#' \code{\link{dbi_database}} when initializing a new \code{dbi_database}.
#'
#' An \code{information_schema} is an \code{\link[base]{environment}} that
#' must contain
#'  \enumerate{
#'  \item a \code{\link[DBI]{DBIConnection-class}} named \code{.dbi_connection},
#'  \item a \code{\link[data.table]{data.table}} or \code{\link{dbi.table}}
#'        named \code{TABLES}, and
#'  \item a \code{\link[data.table]{data.table}} or \code{\link{dbi.table}}
#'        named \code{COLUMNS}.
#' }
#' The \code{TABLES} table must have a column named \code{TABLE_NAME} and should
#' contain columns named \code{TABLE_CATALOG} and \code{TABLE_SCHEMA} if at all
#' possible. Missing values are not allowed.
#'
#' The \code{COLUMNS} table must have columns named \code{TABLE_NAME},
#' \code{COLUMN_NAME}, and \code{ORDINAL_POSITION}. If \code{TABLES} has columns
#' \code{TABLE_CATALOG} and \code{TABLE_SCHEMA} then these must be present in
#' \code{COLUMNS} as well. Missing values are not allowed.
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
#' @return an \code{\link[base]{environment}} containing the components
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

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(info_s, information_schema_disconnect, onexit = TRUE)
  }

  bare_bones_tables <- c("TABLES", "COLUMNS")

  #' @importFrom DBI Id
  ids <- lapply(bare_bones_tables,
                function(u) Id(schema = "INFORMATION_SCHEMA", table = u))

  #' @importFrom DBI dbExistsTable
  has_bare_bones <- all(mapply(dbExistsTable, name = ids,
                               MoreArgs = list(conn = conn)))

  if (has_bare_bones) {
    x <- mapply(new_dbi_table, id = ids, MoreArgs = list(conn = conn),
                SIMPLIFY = FALSE, USE.NAMES = FALSE)

    dev_null <- mapply(assign, x = bare_bones_tables, value = x,
                       MoreArgs = list(pos = info_s))
  } else {
    bare_bones_information_schema(info_s)
  }

  for (nm in ls(info_s)) {
    names(info_s[[nm]]) <- toupper(names(info_s[[nm]]))
  }

  info_s
}



information_schema_disconnect <- function(e) {
  on.exit(rm(list = ".dbi_connection", envir = e))
  #' @importFrom DBI dbDisconnect
  try(dbDisconnect(e[[".dbi_connection"]]), silent = TRUE)
}



bare_bones_information_schema <- function(info_s) {
  conn <- info_s[[".dbi_connection"]]
  #' @importFrom DBI dbListTables
  TABLES <- data.table(TABLE_NAME	= dbListTables(conn),
                       TABLE_TYPE = "BASE TABLE")

  assign("TABLES", TABLES, pos = info_s)

  COLUMNS <- mapply(dbListFields, name = TABLES$TABLE_NAME,
                    MoreArgs = list(conn = conn), SIMPLIFY = FALSE)
  COLUMNS <- lapply(COLUMNS, function(u) data.table(COLUMN_NAME = u))
  COLUMNS <- rbindlist(COLUMNS, idcol = "TABLE_NAME")
  COLUMNS[, ORDINAL_POSITION := seq_len(.N), by = list(TABLE_NAME)]
  setcolorder(COLUMNS, c("TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION"))

  assign("COLUMNS", COLUMNS, pos = info_s)

  invisible()
}



# Define globally for R CMD check
TABLE_CATALOG <- NULL
TABLE_NAME <- NULL
TABLE_SCHEMA <- NULL
COLUMN_NAME <- NULL
ORDINAL_POSITION <- NULL

