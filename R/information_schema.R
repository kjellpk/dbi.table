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
#' The \code{TABLES} table must have a column named \code{table_name} and should
#' contain columns named \code{table_catalog} and \code{table_schema} if at all
#' possible. Missing values are not allowed.
#'
#' The \code{columns} table must have columns named \code{table_name},
#' \code{column_name}, and \code{ordinal_position}. If \code{tables} has columns
#' \code{table_catalog} and \code{table_schema} then these must be present in
#' \code{columns} as well. Missing values are not allowed.
#'
#' \code{information_schema} is an S3 generic. The default method uses the
#' RDBMS's Information Schema if it exists (in which case \code{tables} and
#' \code{columns} are \code{\link{dbi.table}}s). Otherwise, it uses
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
  info <- new.env(parent = emptyenv())
  assign(".dbi_connection", conn, pos = info)

  if (!is.null(attr(conn, "recon", exact = TRUE))) {
    reg.finalizer(info, information_schema_disconnect, onexit = TRUE)
  }

  init_cols <- try(dbGetQuery(conn, "SELECT * FROM information_schema.columns"),
                   silent = TRUE)

  if (is.data.frame(init_cols)) {
    setDT(init_cols)
    setnames(init_cols, tolower(names(init_cols)))

    if (nrow(ss <- init_cols[tolower(table_schema) == "information_schema"])) {
      for (tab in unique(ss$table_name)) {
        tmp <- ss[table_name == tab][order(ordinal_position)]
        id <- tmp[1, list(catalog = table_catalog,
                          schema = table_schema,
                          table = table_name)]
        id <- DBI::Id(unlist(id))
        assign(tab, new_dbi_table(info, id, tmp$column_name), info)
      }
    } else {
      info_tables <- c("columns",
                       "key_column_usage",
                       "referential_constraints",
                       "tables")
      for (tab in info_tables) {
        id <- DBI::SQL(paste0("information_schema.", tab))
        assign(tab, new_dbi_table(info, id), info)
      }
    }
  } else {
    bare_bones_information_schema(info)
  }

  for (nm in ls(info)) {
    names(info[[nm]]) <- tolower(names(info[[nm]]))
  }

  if (is.data.table(init_cols)) {
    info$.init_cols <- init_cols
  }

  info
}



information_schema_disconnect <- function(e) {
  on.exit(rm(list = ".dbi_connection", envir = e))
  #' @importFrom DBI dbDisconnect
  try(dbDisconnect(e[[".dbi_connection"]]), silent = TRUE)
}



bare_bones_information_schema <- function(info) {
  conn <- info$.dbi_connection
  #' @importFrom DBI dbListTables
  tables <- data.table(table_name	= dbListTables(conn),
                       table_type = "BASE TABLE")

  assign("tables", tables, pos = info)

  columns <- mapply(dbListFields, name = tables$table_name,
                    MoreArgs = list(conn = conn), SIMPLIFY = FALSE)
  columns <- lapply(columns, function(u) data.table(column_name = u))
  columns <- rbindlist(columns, idcol = "table_name")
  columns[, ordinal_position := seq_len(.N), by = list(table_name)]
  setcolorder(columns, c("table_name", "column_name", "ordinal_position"))

  assign("columns", columns, pos = info)

  invisible()
}



get_information_schema <- function(x) {
  conn <- get_connection(x)
  if (is.environment(conn) && exists(".dbi_connection", where = conn)) {
    return(conn)
  }

  NULL
}


# Define globally for R CMD check
table_catalog <- NULL
table_schema <- NULL
table_name <- NULL
column_name <- NULL
ordinal_position <- NULL

