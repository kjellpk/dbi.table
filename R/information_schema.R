information_schema <- function(conn, db) {
  info <- new_schema(name = "information_schema", catalog = db)

  init_cols <- try(DBI::dbGetQuery(conn,
                                   "SELECT * FROM information_schema.columns"),
                   silent = TRUE)

  if (is.data.frame(init_cols)) {
    setDT(init_cols)
    setnames(init_cols, tolower(names(init_cols)))

    if (nrow(ss <- init_cols[tolower(table_schema) == "information_schema"])) {
      for (tab in unique(ss$table_name)) {
        tmp <- ss[table_name == tab][order(ordinal_position)]
        id <- tmp[1L, list(table_catalog, table_schema, table_name)]
        id <- DBI::Id(unlist(id))
        info[[tolower(tab)]] <- new_dbi_table(db, id, tolower(tmp$column_name))
      }
    } else {
      info_tables <- c("columns",
                       "key_column_usage",
                       "referential_constraints",
                       "tables")
      for (tab in info_tables) {
        id <- DBI::SQL(paste0("information_schema.", tab))
        info[[tab]] <- new_dbi_table(db, id)
      }
    }
  } else {
    bare_bones_information_schema(conn, info)
  }

  for (nm in ls(info)) {
    names(info[[nm]]) <- tolower(names(info[[nm]]))
  }

  if (is.data.table(init_cols)) {
    info$.init_cols <- init_cols
  }

  info
}



bare_bones_information_schema <- function(conn, info) {
  tables <- data.table(table_name	= DBI::dbListTables(conn),
                       table_type = "BASE TABLE")

  info$tables <- tables

  columns <- mapply(DBI::dbListFields, name = tables$table_name,
                    MoreArgs = list(conn = conn), SIMPLIFY = FALSE)
  columns <- lapply(columns, function(u) data.table(column_name = u))
  columns <- rbindlist(columns, idcol = "table_name")
  columns[, ordinal_position := seq_len(.N), by = list(table_name)]
  setcolorder(columns, c("table_name", "column_name", "ordinal_position"))

  info$columns <- columns

  invisible(info)
}



get_information_schema <- function(x) {
  if (is_dbi_catalog(conn <- get_connection(x))) {
    return(conn$information_schema)
  }

  NULL
}


# Define globally for R CMD check
table_catalog <- NULL
table_schema <- NULL
table_name <- NULL
column_name <- NULL
ordinal_position <- NULL
