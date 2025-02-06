information_schema <- function(catalog, columns) {
  if (!("table_schema" %in% names(columns))) {
    return(NULL)
  }

  info <- new_schema(schema_name = "information_schema", catalog = catalog)
  info_columns <- subset(columns,
                         subset = tolower(table_schema) == "information_schema",
                         select = c("table_catalog",
                                    "table_schema",
                                    "table_name",
                                    "column_name",
                                    "ordinal_position"))

  if (nrow(info_columns)) {
    install_from_columns(info_columns, list(information_schema = info),
                         catalog, to_lower = TRUE)
  } else {
    lapply(session$default_information_schema_tables, function(u) {
      id <- DBI::SQL(paste0("information_schema.", u))
      info_table <- try(new_dbi_table(catalog, id), silent = TRUE)
      if (is.dbi.table(info_table)) {
        names(info_table) <- tolower(names(info_table))
        assign_and_lock(u, info_table, info)
      } else {
        FALSE
      }
    })
  }

  NULL
}



bare_bones_information_schema <- function(catalog) {
  conn <- dbi_connection(catalog)
  info <- new_schema("information_schema", catalog)

  tables <- data.frame(table_name = DBI::dbListTables(conn),
                       table_type = "BASE TABLE")

  columns <- mapply(function(conn, name) {
    fields <- DBI::dbListFields(conn, name)
    data.frame(table_name = name,
               column_name = fields,
               ordinal_position = seq_along(fields))
  },
  name = tables$table_name,
  MoreArgs = list(conn = conn), SIMPLIFY = FALSE)

  columns <- do.call(rbind, columns)

  tables <- as.dbi.table(catalog, tables, type = "query")
  assign_and_lock("tables", tables, info)
  columns <- as.dbi.table(catalog, columns, type = "query")
  assign_and_lock("columns", columns, info)

  info
}



get_information_schema <- function(x) {
  if (is_dbi_catalog(conn <- get_connection(x))) {
    return(conn$information_schema)
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
