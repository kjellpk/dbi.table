### MariaDB ####################################################################

related_tables.MariaDBConnection <- function(conn, x, y = NULL) {
  info <- get_information_schema(x)

  if (is.null(info$referential_constraints) || is.null(info$key_column_usage)) {
    return(data.table())
  }

  r <- merge(info$referential_constraints, info$key_column_usage,
             by = c("constraint_catalog",
                    "constraint_schema",
                    "constraint_name",
                    "table_name",
                    "referenced_table_name"))

  names(r) <- paste0("fk_", names(r))

  r <- merge(r, info$key_column_usage,
             by.x = c("fk_unique_constraint_catalog",
                      "fk_unique_constraint_schema",
                      "fk_unique_constraint_name",
                      "fk_referenced_table_name",
                      "fk_ordinal_position"),
             by.y = c("constraint_catalog",
                      "constraint_schema",
                      "constraint_name",
                      "table_name",
                      "ordinal_position"))

  r <- r[, list(constraint = fk_constraint_name,
                catalog_x = fk_table_catalog,
                schema_x = fk_table_schema,
                table_x = fk_table_name,
                field_x = fk_column_name,
                catalog_y = table_catalog,
                schema_y = table_schema,
                table_y = fk_referenced_table_name,
                field_y = column_name)]

  xids <- unique(get_data_source(x)$id)
  xids <- as.data.table(t(sapply(xids, function(u) u@name)))

  setnames(xids, c("catalog_x", "schema_x", "table_x"))

  rx <- as.data.table(r[catalog_x %in% xids$catalog_x &
                          schema_x %in% xids$schema_x &
                          table_x %in% xids$table_x])

  rx <- rx[xids, nomatch = NULL, on = names(xids)]

  setnames(xids, c("catalog_y", "schema_y", "table_y"))
  rx <- rx[!xids, on = names(xids)]

  if (!is.null(y)) {
    yids <- unique(get_data_source(y)$id)
    yids <- as.data.table(t(sapply(yids, function(u) u@name)))
    setnames(yids, c("catalog_y", "schema_y", "table_y"))

    rx <- rx[yids, nomatch = NULL, on = names(yids)]

    setnames(yids, c("catalog_x", "schema_x", "table_x"))

    ry <- as.data.table(r[catalog_x %in% yids$catalog_x &
                            schema_x %in% yids$schema_x &
                            table_x %in% yids$table_x])

    ry <- ry[yids, nomatch = NULL, on = names(yids)]
    ry <- ry[xids, nomatch = NULL, on = names(xids)]

    setnames(ry,
             c("catalog_x", "schema_x", "table_x", "field_x",
               "catalog_y", "schema_y", "table_y", "field_y"),
             c("catalog_y", "schema_y", "table_y", "field_y",
               "catalog_x", "schema_x", "table_x", "field_x"))
    setcolorder(ry, names(rx))
    rx <- rbind(rx, ry)
  }

  rx
}



fk_referenced_table_name <- NULL
