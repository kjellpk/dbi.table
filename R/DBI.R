################################################################################
#' Compact Data Frame
#'
#' @description Store database \emph{Info} in a compact data structure
#'              indexed by an \code{\link[DBI]{Id}}.
#'
#' @param x a \code{\link{data.frame}} where one or more of the columns
#'          identifies a \emph{table}.
#'
#' @param idcols a character vector specifying the subset of the columns
#'               of \code{x} that will be passed to \code{\link[DBI]{Id}}.
#'
#' @export
split_by_id <- function(x, idcols) {
  keep <- setdiff(names(x), idcols)
  x <- split(x, f = x[, idcols])

  ids <- unname(lapply(x, `[`, i = 1, j = idcols, drop = FALSE))
  #' @importFrom DBI Id
  ids <- lapply(ids, function(u) Id(unlist(as.list(u))))

  cols <- list()
  for (k in keep) {
    u <- unname(lapply(x, `[[`, i = k))
    cols[[k]] <- u[!is.na(u) & nchar(u) > 0]
  }

  do.call(data.frame, c(list(id = I(ids)), lapply(cols, I)))
}



################################################################################
#' List Database Schema
#'
#' @description List a database schema by table including column names.
#'
#' @param conn a \code{DBIConnection}.
#'
#' @param prefix an \code{\link[DBI]{Id}} specifying fully qualified path
#'               in the database's namespace, or NULL.
#'
#' @param \dots additional arguements.
#'
#' @section Value: a compact representation of the database schema.
#'
#' @rdname dbListSchema
#'
#' @export
setGeneric(name = "dbListSchema",
           def = function(conn, prefix = NULL, ...) {
                   standardGeneric("dbListSchema")
                 },
           valueClass = "data.frame",
           signature = "conn")

#' @importFrom methods setGeneric .valueClassTest



setMethod(f = "dbListSchema",
          signature = c(conn = "DBIConnection"),
          definition = function(conn, prefix = NULL, ...) {

  #' @importFrom DBI dbListObjects
  schema <- dbListObjects(conn, prefix = prefix, ...)
  schema <- schema[!schema$is_prefix, "table", drop = FALSE]
  names(schema) <- "id"

  #' @importFrom DBI dbListFields
  fields <- lapply(schema$id, function(u, v) dbListFields(v, u), v = conn)

  cbind(schema, column_names = I(fields))
})



#' Foreign Keys
#'
#' @description List the foreign keys of a database table.
#'
#' @param conn a \code{DBIConnection}.
#'
#' @param id an \code{\link[DBI]{Id}} specifying a database table.
#'
#' @param \dots additional arguements.
#'
#' @section Value: a compact representation of the foreign keys.
#'
#' @rdname dbListForeignKeys
#'
#' @export
setGeneric(name = "dbForeignKeys",
           valueClass = "data.frame",
           def = function(conn, id, ...) standardGeneric("dbForeignKeys"))

#' @importFrom methods setGeneric .valueClassTest



#' @importFrom methods setMethod
setMethod(f = "dbForeignKeys",
          signature = c(conn = "DBIConnection", id = "Id"),
          definition = function(conn, id, ...) {
  data.frame(id = I(list()), primary = I(list()), foreign = I(list()))
})
