#' @importFrom methods setOldClass
#' @export
setOldClass("dbi.catalog")



#' @importFrom methods setOldClass
#' @export
setOldClass("dbi.schema")



#' @importFrom methods setOldClass
#' @export
setOldClass("dbi.table")



################################################################################
#' @name
#'   dbi.table.DBI
#'
#' @title
#'   DBI Methods for \code{dbi.table}s
#'
#' @description
#'   Call DBI methods using the underlying DBI connection.
#'
#' @param conn
#'   a \code{\link{dbi.catalog}}, \code{dbi.schema}, or \code{\link{dbi.table}}.
#'
#' @param dbObj
#'   a \code{\link{dbi.catalog}}, \code{dbi.schema}, or \code{\link{dbi.table}}.
#'
#' @param statement
#'   a \code{\link[DBI]{SQL}} object.
#'
#' @param \dots
#'   other parameters passed on to methods.
#'
#' @seealso
#'   \code{\link[DBI]{dbExecute}}, \code{\link[DBI]{dbGetInfo}},
#'   \code{\link[DBI]{dbSendStatement}}
#'
#' @docType methods
#'
#' @rdname DBI-methods
NULL



# dbAppendTable
# dbAppendTableArrow
# dbBegin
# dbBind
# dbBindArrow
# dbBreak
# dbCallProc
# dbCanConnect
# dbClearResult
# dbColumnInfo
# dbCommit
# dbConnect
# dbCreateTable
# dbCreateTableArrow
# dbDataType
# dbDisconnect
# dbDriver

################################################################################
# dbExecute
################################################################################

dbExecute_dbi_table_pkg <- function(conn, statement, ...) {
  DBI::dbExecute(dbi_connection(conn), statement, ...)
}



#' @docType methods
#' @rdname DBI-methods
#' @aliases dbExecute,dbi.catalog,ANY-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.catalog", "ANY"),
          definition = dbExecute_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbExecute,dbi.schema,ANY-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.schema", "ANY"),
          definition = dbExecute_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbExecute,dbi.table,ANY-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = c("dbi.table", "ANY"),
          definition = dbExecute_dbi_table_pkg)



################################################################################

# dbExistsTable
# dbFetch
# dbFetchArrow
# dbFetchArrowChunk
# dbGetConnectArgs
# dbGetDBIVersion
# dbGetException

################################################################################
# dbGetInfo
################################################################################

dbGetInfo_dbi_table_pkg <- function(dbObj, ...) {
  DBI::dbGetInfo(dbi_connection(dbObj), ...)
}



#' @rdname DBI-methods
#' @aliases dbGetInfo,dbi.catalog
#' @importFrom DBI dbGetInfo
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetInfo,
          signature = "dbi.catalog",
          definition = dbGetInfo_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetInfo,dbi.schema
#' @importFrom DBI dbGetInfo
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetInfo,
          signature = "dbi.schema",
          definition = dbGetInfo_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetInfo,dbi.table
#' @importFrom DBI dbGetInfo
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetInfo,
          signature = "dbi.table",
          definition = dbGetInfo_dbi_table_pkg)



################################################################################
# dbGetQuery
################################################################################

dbGetQuery_dbi_table <- function(conn, statement, ...) {
  if (is.null(n <- list(...)$n)) {
    n <- getOption("dbitable.max.fetch", 10000L)
  }
  DBI::dbGetQuery(dbi_connection(conn), write_select_query(conn, n), n = n)
}



#' @rdname DBI-methods
#' @aliases dbGetQuery,dbi.table,missing-method
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetQuery,
          signature = c("dbi.table", "missing"),
          definition = dbGetQuery_dbi_table)



dbGetQuery_dbi_table_pkg <- function(conn, statement, ...) {
  DBI::dbGetQuery(dbi_connection(conn), statement, ...)
}



#' @rdname DBI-methods
#' @aliases dbGetQuery,dbi.catalog,ANY-method
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetQuery,
          signature = c("dbi.catalog", "ANY"),
          definition = dbGetQuery_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetQuery,dbi.schema,ANY-method
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetQuery,
          signature = c("dbi.schema", "ANY"),
          definition = dbGetQuery_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetQuery,dbi.table,ANY-method
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetQuery,
          signature = c("dbi.table", "ANY"),
          definition = dbGetQuery_dbi_table_pkg)



################################################################################

# dbGetQueryArrow
# dbGetRowCount
# dbGetRowsAffected
# dbGetStatement
# dbHasCompleted
# dbIsReadOnly
# dbIsValid
# dbListConnections
# dbListFields
# dbListObjects
# dbListResults
# dbListTables
# dbQuoteIdentifier
# dbQuoteLiteral
# dbQuoteString
# dbReadTable
# dbReadTableArrow
# dbRemoveTable
# dbRollback
# dbSendQuery
# dbSendQueryArrow

################################################################################
# dbSendStatement
################################################################################

dbSendStatement_dbi_table <- function(conn, statement, ...) {
  if (is.null(n <- list(...)$n)) {
    n <- getOption("dbitable.max.fetch", 10000L)
  }

  DBI::dbSendStatement(dbi_connection(conn), write_select_query(conn, n))
}



#' @rdname DBI-methods
#' @aliases dbSendStatement,dbi.table,missing-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = c("dbi.table", "missing"),
          definition = dbSendStatement_dbi_table)



dbSendStatement_dbi_table_pkg <- function(conn, statement, ...) {
  DBI::dbSendStatement(dbi_connection(conn), statement, ...)
}



#' @rdname DBI-methods
#' @aliases dbSendStatement,dbi.catalog,ANY-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = c("dbi.catalog", "ANY"),
          definition = dbSendStatement_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbSendStatement,dbi.schema,ANY-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = c("dbi.schema", "ANY"),
          definition = dbSendStatement_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbSendStatement,dbi.table,ANY-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = c("dbi.table", "ANY"),
          definition = dbSendStatement_dbi_table_pkg)



################################################################################

# dbSetDataMappings
# dbUnloadDriver
# dbUnquoteIdentifier
# dbWithTransaction
# dbWriteTable
# dbWriteTableArrow
# fetch
# Id
# isSQLKeyword
# isSQLKeyword.default
# make.db.names
# make.db.names.default
# show
# SQL
# sqlAppendTable
# sqlAppendTableTemplate
# sqlColumnToRownames
# sqlCommentSpec
# sqlCreateTable
# sqlData
# sqlInterpolate
# SQLKeywords
# sqlParseVariables
# sqlParseVariablesImpl
# sqlQuoteSpec
# sqlRownamesToColumn
