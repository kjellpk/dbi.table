#' Attach a DBI Database Connection to the Search Path
#'
#' @description Create an \code{DBI.table} object (representing a table in
#'              a SQL database) for each table in \code{conn}.
#'
#' @param conn a connection handle returned by \code{\link[DBI]{dbConnect}}.
#'
#' @param pos an integer specifying position in \code{\link[base]{search}} ()
#'            where to attach.
#'
#' @param name a character string specifying the name to use for the attached
#'             database. The
#'
#' @param warn.conflicts a logical value. If \code{TRUE}, warnings are
#'                       printed about \code{\link[base]{conflicts}} from
#'                       attaching the database, unless that database contains
#'                       an object \code{.conflicts.OK}. A conflict is a
#'                       function masking a function, or a non-function
#'                       masking a non-function.
#'
#' @param \dots additional arguments are passed to
#'              \code{\link[DBI]{dbListTables}} (with the intention of filtering
#'              which tables are loaded into the attached environment).
#'
#' @export
dbAttach<- function(conn, pos = 2L, name = conn@dbname,
                    warn.conflicts = FALSE, ...)
{
  name <- paste(attr(class(con), "package"), name, sep = ":")
  e <- attach(NULL, pos = pos, name = name, warn.conflicts = FALSE)
  e$.channel <- conn

  for(table.name in DBI::dbListTables(e$.channel, ...)) {
    x <- list(name = table.name, fields = NULL, envir = e)
    class(x) <- "DBI.table"
    assign(table.name, x, envir = e)
  }

  if(warn.conflicts && !exists(".conflicts.OK", envir = e, inherits = FALSE))
    print(conflicts(detail = TRUE)) #attach uses checkConflicts()

  invisible(e)
}


