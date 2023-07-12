################################################################################
# Compact Storage for Schema, etc.

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
# dbListSchema

setGeneric(name = "dbListSchema",
           def = function(conn, prefix = NULL, ...) {
                   standardGeneric("dbListSchema")
                 },
           valueClass = "data.frame",
           signature = "conn")

#' @importFrom methods setGeneric .valueClassTest



dbListSchema_default <- function(conn, prefix = NULL, ...) {

  #' @importFrom DBI dbListObjects
  schema <- dbListObjects(conn, prefix = prefix, ...)
  schema <- schema[!schema$is_prefix, "table", drop = FALSE]
  names(schema) <- "id"

  #' @importFrom DBI dbListFields
  fields <- lapply(schema$id, function(u, v) dbListFields(v, u), v = conn)

  cbind(schema, column_names = I(fields))
}



setMethod(f = "dbListSchema",
          signature = c(conn = "DBIConnection"),
          definition = dbListSchema_default)



dbListSchema_Microsoft_SQL_Server <- function(conn, prefix = NULL, ...) {

  if (!is.null(prefix)) {
    components <- prefix@name
  } else {
    components <- character()
  }

  if (is.na(catalog <- components["catalog"])) {
    #' @importFrom DBI dbGetInfo
    catalog <- dbGetInfo(conn)$dbname
  }

  q1 <- "SELECT \"TABLE_CATALOG\" AS \"catalog\",
                \"TABLE_SCHEMA\" AS \"schema\",
                \"TABLE_NAME\" AS \"table\",
                \"COLUMN_NAME\" AS \"column_names\"
           FROM %s.\"INFORMATION_SCHEMA\".\"COLUMNS\""

  if (!is.na(schema <- components["schema"])) {
    q1 <- paste0(q1, "\nWHERE \"TABLE_SCHEMA\" = '", schema, "'")
  }

  q1 <- paste0(q1, "\nORDER BY \"TABLE_CATALOG\", \"TABLE_SCHEMA\", \"TABLE_NAME\", \"ORDINAL_POSITION\"")
  #' @importFrom DBI dbQuoteIdentifier
  q1 <- sprintf(q1, dbQuoteIdentifier(conn, catalog))

  #' @importFrom DBI dbGetQuery
  split_by_id(dbGetQuery(conn, q1), c("catalog", "schema", "table"))
}



#' @importFrom methods setMethod
setMethod(f = "dbListSchema",
          signature = c(conn = "Microsoft SQL Server"),
          definition = dbListSchema_Microsoft_SQL_Server)



################################################################################
# dbForeignKeys

setGeneric(name = "dbForeignKeys",
           valueClass = "data.frame",
           def = function(conn, id, ...) standardGeneric("dbForeignKeys"))

#' @importFrom methods setGeneric .valueClassTest



dbForeignKeys_default <- function(conn, id, ...) {
  data.frame(id = I(list()), primary = I(list()), foreign = I(list()))
}



#' @importFrom methods setMethod
setMethod(f = "dbForeignKeys",
          signature = c(conn = "DBIConnection", id = "Id"),
          definition = dbForeignKeys_default)



dbForeignKeys_SQLiteConnection <- function(conn, id, ...) {

  query <- "SELECT `table`, `to` AS `primary`, `from` AS `foreign`
              FROM pragma_foreign_key_list(%s)
             ORDER BY `id`, `seq`;"
  #' @importFrom DBI dbQuoteLiteral
  query <- sprintf(query, dbQuoteLiteral(conn, id@name["table"]))

  #' @importFrom DBI dbGetQuery
  split_by_id(dbGetQuery(conn, query), "table")
}



#' @importFrom methods setMethod
setMethod(f = "dbForeignKeys",
          signature = c(conn = "SQLiteConnection", id = "Id"),
          definition = dbForeignKeys_SQLiteConnection)



dbForeignKeys_Microsoft_SQL_Server <- function(conn, id, ...) {
  components <- id@name

  xref <- c(catalog = "@fktable_qualifier",
            schema = "@fktable_owner",
            table = "@fktable_name")

  names(components) <- xref[names(components)]

  #' @importFrom DBI dbQuoteString
  statement <- paste(names(components), dbQuoteString(conn, components), sep = " = ")
  statement <- paste("EXEC sp_fkeys", paste(statement, collapse = ", "))

  #' @importFrom DBI dbGetQuery
  fk <- dbGetQuery(conn, statement)
  fk <- fk[, c("PKTABLE_QUALIFIER", "PKTABLE_OWNER", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKCOLUMN_NAME")]
  names(fk) <- c("catalog", "schema", "table", "primary", "foreign")

  split_by_id(fk, c("catalog", "schema", "table"))
}



#' @importFrom methods setMethod
setMethod(f = "dbForeignKeys",
          signature = c(conn = "Microsoft SQL Server", id = "Id"),
          definition = dbForeignKeys_Microsoft_SQL_Server)
