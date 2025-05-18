tables_schema <- function(catalog) {
  schema <- tables_schema_(catalog)

  if (isTRUE(attr(schema, "default_method", exact = TRUE))) {
    n <- ncol(schema)
    m <- nrow(schema)

    table_name <- schema[, n]

    if (n > 1L) {
      table_schema <- schema[, n - 1L]
    } else {
      table_schema <- rep("main", m)
    }

    id <- apply(schema, 1L, DBI::Id)
    fields <- rep(list(character(0)), nrow(schema))
    key <- vector(nrow(schema), mode = "list")

    schema <- data.frame(table_schema = table_schema,
                         table_name = table_name,
                         id = I(id),
                         fields = I(fields),
                         key = I(key))
  } else {
    id_cols <- seq_len(match("column_name", names(schema)) - 1L)
    schema$f <- apply(schema[, id_cols, drop = FALSE], 1L,
                      paste, collapse = ".")
    schemata <- split(schema, schema$f)

    schemata <- lapply(schemata, function(u, v) {
      table_schema <- u[[1L, "table_schema"]]
      table_name <- u[[1L, "table_name"]]
      id <- DBI::Id(unlist(u[1L, v]))
      fields <- u$column_name[as.integer(u$ordinal_position)]

      if (nrow(u <- u[!is.na(u$pk_ordinal_position), ])) {
        key <- u$column_name[as.integer(u$pk_ordinal_position)]
      } else {
        key <- NULL
      }

      data.frame(table_schema = table_schema,
                 table_name = table_name,
                 id = I(list(id)),
                 fields = I(list(fields)),
                 key = I(list(key)))
    }, v = id_cols)

    names(schemata) <- NULL
    schema <- do.call(rbind, schemata)
  }

  schema
}



tables_schema_ <- function(catalog) {
  UseMethod("tables_schema_")
}



#' @rawNamespace S3method(tables_schema_,default,tables_schema_default)
tables_schema_default <- function(catalog) {
  objs <- DBI::dbListObjects(catalog)
  objs <- objs[objs$is_prefix == FALSE, ]
  columns <- lapply(objs$table, function(u) u@name)
  x <- as.data.frame(t(simplify2array(columns, higher = FALSE, except = NULL)))
  attr(x, "default_method") <- TRUE
  x
}
