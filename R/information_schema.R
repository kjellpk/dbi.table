information_schema <- function(catalog, columns) {
  info <- new_schema(schema_name = "information_schema", catalog = catalog)

  if (nrow(ss <- columns[tolower(table_schema) == "information_schema"])) {
    for (tab in unique(ss$table_name)) {
      tmp <- ss[table_name == tab][order(ordinal_position)]
      id <- tmp[1L, list(table_catalog, table_schema, table_name)]
      id <- DBI::Id(unlist(id))
      assign(tolower(tab),
             new_dbi_table(catalog, id, tolower(tmp$column_name)),
             info)
    }
  } else {
    info_tables <- c("columns",
                     "key_column_usage",
                     "referential_constraints",
                     "tables")
    for (info_table in info_tables) {
      id <- DBI::SQL(paste0("information_schema.", info_table))
      tab <- try(new_dbi_table(catalog, id), silent = TRUE)
      if (is.dbi.table(tab)) {
        assign(info_table, tab, info)
      }
    }
  }

  info
}



bare_bones_information_schema <- function(catalog) {
  conn <- dbi_connection(catalog)
  info <- new_schema("information_schema", catalog)

  tables <- data.table(table_name = DBI::dbListTables(conn),
                       table_type = "BASE TABLE")

  assign("tables", tables, info)
  lockBinding("tables", info)

  columns <- mapply(DBI::dbListFields, name = tables$table_name,
                    MoreArgs = list(conn = conn), SIMPLIFY = FALSE)
  columns <- lapply(columns, function(u) data.table(column_name = u))
  columns <- rbindlist(columns, idcol = "table_name")
  columns[, ordinal_position := seq_len(.N), by = .(table_name)]
  setcolorder(columns, c("table_name",
                         "column_name",
                         "ordinal_position"))

  assign("columns", columns, info)
  lockBinding("columns", info)

  info
}



get_information_schema <- function(x) {
  if (is_dbi_catalog(conn <- get_connection(x))) {
    return(conn$information_schema)
  }

  NULL
}



get_init_columns <- function(catalog) {
  columns <- try(DBI::dbGetQuery(dbi_connection(catalog),
                                 "SELECT * FROM information_schema.columns"),
                 silent = TRUE)

  if (is.data.frame(columns)) {
    setDT(columns)
    setnames(columns, tolower(names(columns)))
    setkeyv(columns, c("table_catalog",
                       "table_schema",
                       "table_name",
                       "ordinal_position"))
    return(columns)
  }

  NULL
}



# Define globally for R CMD check
. <- NULL
table_catalog <- NULL
table_schema <- NULL
table_name <- NULL
column_name <- NULL
ordinal_position <- NULL
