information_schema <- function(catalog, columns) {
  info <- new_schema(schema_name = "information_schema", catalog = catalog)
  info_columns <- columns[tolower(table_schema) == "information_schema"]

  if (nrow(info_columns)) {
    install_from_columns(info_columns, list(information_schema = info),
                         catalog, to_lower = TRUE)
  } else {
    dev_null <- lapply(session$default_information_schema_tables, function(u) {
      id <- DBI::SQL(paste0("information_schema.", u))
      info_table <- try(new_dbi_table(catalog, id), silent = TRUE)
      if (is.dbi.table(info_table)) assign_and_lock(u, info_table, info)
    })
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
  columns[, ordinal_position := seq_len(.N), by = "table_name"]
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
dbi_table <- NULL
table_catalog <- NULL
table_schema <- NULL
table_name <- NULL
column_name <- NULL
ordinal_position <- NULL
