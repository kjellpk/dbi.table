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

stry <- function(expr) {
  suppressWarnings(try(expr, silent = TRUE))
}



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
#'   A \code{\link{dbi.catalog}}, \code{dbi.schema}, or \code{\link{dbi.table}}.
#'
#' @param dbObj
#'   A \code{\link{dbi.catalog}}, \code{dbi.schema}, or \code{\link{dbi.table}}.
#'
#' @param name
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).
#'
#' @param fields
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).
#'
#' @param value
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).
#'
#' @param statement
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).
#'
#' @param code
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).
#'
#' @param x
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).

#' @param row.names
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).
#'
#' @param temporary
#'   Please refer to the documentation for the generic function (links can be
#'   found in the 'See Also' section).
#'
#' @param \dots
#'   Additional parameters to pass to methods.
#'
#' @seealso
#'   \code{\link[DBI]{dbAppendTable}},
#'   \code{\link[DBI]{dbCreateTable}},
#'   \code{\link[DBI]{dbExecute}},
#'   \code{\link[DBI]{dbGetInfo}},
#'   \code{\link[DBI]{dbGetQuery}},
#'   \code{\link[DBI]{dbReadTable}},
#'   \code{\link[DBI]{dbQuoteIdentifier}},
#'   \code{\link[DBI]{dbQuoteLiteral}},
#'   \code{\link[DBI]{dbQuoteString}},
#'   \code{\link[DBI]{dbRemoveTable}},
#'   \code{\link[DBI]{dbSendStatement}},
#'   \code{\link[DBI]{dbWithTransaction}}
#'
#' @docType methods
#'
#' @rdname DBI-methods
NULL


################################################################################
# dbAppendTable
################################################################################

dbAppendTable_dbi_table_pkg <- function(conn, name, value, ...,
                                        row.names = NULL) {
  DBI::dbAppendTable(dbi_connection(conn), name, value, ...,
                     row.names = row.names)
}



#' @docType methods
#' @rdname DBI-methods
#' @aliases dbAppendTable,dbi.catalog-method
#' @importFrom DBI dbAppendTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbAppendTable,
          signature = "dbi.catalog",
          definition = dbAppendTable_dbi_table_pkg)



#' @docType methods
#' @rdname DBI-methods
#' @aliases dbAppendTable,dbi.schema-method
#' @importFrom DBI dbAppendTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbAppendTable,
          signature = "dbi.schema",
          definition = dbAppendTable_dbi_table_pkg)



#' @docType methods
#' @rdname DBI-methods
#' @aliases dbAppendTable,dbi.table-method
#' @importFrom DBI dbAppendTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbAppendTable,
          signature = "dbi.table",
          definition = dbAppendTable_dbi_table_pkg)



################################################################################

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

################################################################################
# dbCreateTable
################################################################################

dbCreateTable_dbi_table_pkg <- function(conn, name, fields, ...,
                                        row.names, temporary) {
  DBI::dbCreateTable(dbi_connection(conn), name, fields, ...,
                     row.names = row.names, temporary = temporary)
}



#' @docType methods
#' @rdname DBI-methods
#' @aliases dbCreateTable,dbi.catalog-method
#' @importFrom DBI dbCreateTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbCreateTable,
          signature = "dbi.catalog",
          definition = dbCreateTable_dbi_table_pkg)



#' @docType methods
#' @rdname DBI-methods
#' @aliases dbCreateTable,dbi.schema-method
#' @importFrom DBI dbCreateTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbCreateTable,
          signature = "dbi.schema",
          definition = dbCreateTable_dbi_table_pkg)



#' @docType methods
#' @rdname DBI-methods
#' @aliases dbCreateTable,dbi.table-method
#' @importFrom DBI dbCreateTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbCreateTable,
          signature = "dbi.table",
          definition = dbCreateTable_dbi_table_pkg)



################################################################################

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
#' @aliases dbExecute,dbi.catalog-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = "dbi.catalog",
          definition = dbExecute_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbExecute,dbi.schema-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = "dbi.schema",
          definition = dbExecute_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbExecute,dbi.table-method
#' @importFrom DBI dbExecute
#' @importFrom methods setMethod
#' @export
setMethod(f = dbExecute,
          signature = "dbi.table",
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
#' @aliases dbGetInfo,dbi.catalog-method
#' @importFrom DBI dbGetInfo
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetInfo,
          signature = "dbi.catalog",
          definition = dbGetInfo_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetInfo,dbi.schema-method
#' @importFrom DBI dbGetInfo
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetInfo,
          signature = "dbi.schema",
          definition = dbGetInfo_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetInfo,dbi.table-method
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

  dbGetQuery_dbi_table_pkg(conn, write_select_query(conn, n), n = n)
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
  x <- stry(DBI::dbGetQuery(dbi_conn <- dbi_connection(conn), statement, ...))

  if (inherits(x, "try-error")) {
    if (is_valid <- DBI::dbIsValid(dbi_conn)) {
      simple_query_works <- stry(DBI::dbGetQuery(dbi_conn, "SELECT 1;"))
      is_valid <- !inherits(simple_query_works, "try-error")
    } else {
      stop(attr(x, "condition"))
    }

    if (is_valid) {
      stop(attr(x, "condition"))
    }

    if (is_dbi_catalog(e <- get_connection(conn))) {
      if (!is.null(recon <- attr(dbi_conn, "recon", exact = TRUE))) {
        stry(DBI::dbDisconnect(dbi_conn))
        assign("./dbi_connection", init_connection(recon), pos = e)
      } else {
        stop(attr(x, "condition"))
      }
    }

    x <- stry(DBI::dbGetQuery(dbi_connection(conn), statement, ...))

    if (inherits(x, "try-error")) {
      stop(attr(x, "condition"))
    }
  }

  x
}



#' @rdname DBI-methods
#' @aliases dbGetQuery,dbi.catalog-method
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetQuery,
          signature = "dbi.catalog",
          definition = dbGetQuery_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetQuery,dbi.schema-method
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetQuery,
          signature = "dbi.schema",
          definition = dbGetQuery_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbGetQuery,dbi.table-method
#' @importFrom DBI dbGetQuery
#' @importFrom methods setMethod
#' @export
setMethod(f = dbGetQuery,
          signature = "dbi.table",
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


################################################################################
# dbQuoteIdentifier
################################################################################

dbQuoteIdentifier_dbi_table_pkg <- function(conn, x, ...)  {
  DBI::dbQuoteIdentifier(dbi_connection(conn), x, ...)
}



#' @rdname DBI-methods
#' @aliases dbQuoteIdentifier,dbi.catalog-method
#' @importFrom DBI dbQuoteIdentifier
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteIdentifier,
          signature = "dbi.catalog",
          definition = dbQuoteIdentifier_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbQuoteIdentifier,dbi.schema-method
#' @importFrom DBI dbQuoteIdentifier
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteIdentifier,
          signature = "dbi.schema",
          definition = dbQuoteIdentifier_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbQuoteIdentifier,dbi.table-method
#' @importFrom DBI dbQuoteIdentifier
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteIdentifier,
          signature = "dbi.table",
          definition = dbQuoteIdentifier_dbi_table_pkg)



################################################################################
# dbQuoteLiteral
################################################################################

dbQuoteLiteral_dbi_table_pkg <- function(conn, x, ...)  {
  DBI::dbQuoteLiteral(dbi_connection(conn), x, ...)
}



#' @rdname DBI-methods
#' @aliases dbQuoteLiteral,dbi.catalog-method
#' @importFrom DBI dbQuoteLiteral
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteLiteral,
          signature = "dbi.catalog",
          definition = dbQuoteLiteral_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbQuoteLiteral,dbi.schema-method
#' @importFrom DBI dbQuoteLiteral
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteLiteral,
          signature = "dbi.schema",
          definition = dbQuoteLiteral_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbQuoteLiteral,dbi.table-method
#' @importFrom DBI dbQuoteLiteral
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteLiteral,
          signature = "dbi.table",
          definition = dbQuoteLiteral_dbi_table_pkg)



################################################################################
# dbQuoteString
################################################################################

dbQuoteString_dbi_table_pkg <- function(conn, x, ...)  {
  DBI::dbQuoteString(dbi_connection(conn), x, ...)
}



#' @rdname DBI-methods
#' @aliases dbQuoteString,dbi.catalog-method
#' @importFrom DBI dbQuoteString
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteString,
          signature = "dbi.catalog",
          definition = dbQuoteString_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbQuoteString,dbi.schema-method
#' @importFrom DBI dbQuoteString
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteString,
          signature = "dbi.schema",
          definition = dbQuoteString_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbQuoteString,dbi.table-method
#' @importFrom DBI dbQuoteString
#' @importFrom methods setMethod
#' @export
setMethod(f = dbQuoteString,
          signature = "dbi.table",
          definition = dbQuoteString_dbi_table_pkg)



################################################################################
# dbReadTable
################################################################################

dbReadTable_dbi_table_pkg <- function(conn, name, ...) {
  DBI::dbReadTable(dbi_connection(conn), name, ...)
}



#' @rdname DBI-methods
#' @aliases dbReadTable,dbi.catalog-method
#' @importFrom DBI dbReadTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbReadTable,
          signature = "dbi.catalog",
          definition = dbReadTable_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbReadTable,dbi.schema-method
#' @importFrom DBI dbReadTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbReadTable,
          signature = "dbi.schema",
          definition = dbReadTable_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbReadTable,dbi.table-method
#' @importFrom DBI dbReadTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbReadTable,
          signature = "dbi.table",
          definition = dbReadTable_dbi_table_pkg)



################################################################################

# dbReadTableArrow

################################################################################
# dbRemoveTable
################################################################################

dbRemoveTable_dbi_table_pkg <- function(conn, name, ...) {
  DBI::dbRemoveTable(dbi_connection(conn), name, ...)
}



#' @rdname DBI-methods
#' @aliases dbRemoveTable,dbi.catalog-method
#' @importFrom DBI dbRemoveTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbRemoveTable,
          signature = "dbi.catalog",
          definition = dbRemoveTable_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbRemoveTable,dbi.schema-method
#' @importFrom DBI dbRemoveTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbRemoveTable,
          signature = "dbi.schema",
          definition = dbRemoveTable_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbRemoveTable,dbi.table-method
#' @importFrom DBI dbRemoveTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbRemoveTable,
          signature = "dbi.table",
          definition = dbRemoveTable_dbi_table_pkg)



################################################################################

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
#' @aliases dbSendStatement,dbi.catalog-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = "dbi.catalog",
          definition = dbSendStatement_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbSendStatement,dbi.schema-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = "dbi.schema",
          definition = dbSendStatement_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbSendStatement,dbi.table-method
#' @importFrom DBI dbSendStatement
#' @importFrom methods setMethod
#' @export
setMethod(f = dbSendStatement,
          signature = "dbi.table",
          definition = dbSendStatement_dbi_table_pkg)



################################################################################

# dbSetDataMappings
# dbUnloadDriver
# dbUnquoteIdentifier

################################################################################
# dbWithTransaction
################################################################################

dbWithTransaction_dbi_table_pkg <- function(conn, code, ...) {
  DBI::dbWithTransaction(dbi_connection(conn), code, ...)
}



#' @rdname DBI-methods
#' @aliases dbWithTransaction,dbi.catalog-method
#' @importFrom DBI dbWithTransaction
#' @importFrom methods setMethod
#' @export
setMethod(f = dbWithTransaction,
          signature = "dbi.catalog",
          definition = dbWithTransaction_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbWithTransaction,dbi.schema-method
#' @importFrom DBI dbWithTransaction
#' @importFrom methods setMethod
#' @export
setMethod(f = dbWithTransaction,
          signature = "dbi.schema",
          definition = dbWithTransaction_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbWithTransaction,dbi.table-method
#' @importFrom DBI dbWithTransaction
#' @importFrom methods setMethod
#' @export
setMethod(f = dbWithTransaction,
          signature = "dbi.table",
          definition = dbWithTransaction_dbi_table_pkg)



################################################################################
# dbWriteTable
################################################################################

dbWriteTable_dbi_table_pkg <- function(conn, name, value, ...) {
  DBI::dbWriteTable(dbi_connection(conn), name, value, ...)
}



#' @rdname DBI-methods
#' @aliases dbWriteTable,dbi.catalog-method
#' @importFrom DBI dbWriteTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbWriteTable,
          signature = "dbi.catalog",
          definition = dbWriteTable_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbWriteTable,dbi.schema-method
#' @importFrom DBI dbWriteTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbWriteTable,
          signature = "dbi.schema",
          definition = dbWriteTable_dbi_table_pkg)



#' @rdname DBI-methods
#' @aliases dbWriteTable,dbi.table-method
#' @importFrom DBI dbWriteTable
#' @importFrom methods setMethod
#' @export
setMethod(f = dbWriteTable,
          signature = "dbi.table",
          definition = dbWriteTable_dbi_table_pkg)



################################################################################

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
