related_tables <- function(x, y = NULL) {
  info <- get_information_schema(x)
  if (is.null(info$referential_constraints) || is.null(info$key_column_usage)) {
    return(NULL)
  }

  r <- merge(info$referential_constraints,
             info$key_column_usage,
             by = c("constraint_catalog",
                    "constraint_schema",
                    "constraint_name"))

  names(r) <- paste0("fk_", names(r))

  r <- merge(r,
             info$key_column_usage,
             by.x = c("fk_unique_constraint_catalog",
                      "fk_unique_constraint_schema",
                      "fk_unique_constraint_name",
                      "fk_ordinal_position"),
             by.y = c("constraint_catalog",
                      "constraint_schema",
                      "constraint_name",
                      "ordinal_position"))

  r <- r[, list(constraint = fk_constraint_name,
                catalog_x = fk_table_catalog,
                schema_x = fk_table_schema,
                table_x = fk_table_name,
                field_x = fk_column_name,
                catalog_y = table_catalog,
                schema_y = table_schema,
                table_y = table_name,
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
    dbi_yids <- as.dbi.table(yids, get_connection(r))

    ry <- as.data.table(r[dbi_yids, nomatch = NULL, on = names(dbi_yids)])
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



relational_merge <- function(x) {
  if (is.null(info <- get_information_schema(x))) {
    return(x)
  }

  if (is.null(info$referential_constraints) || is.null(info$key_column_usage)) {
    return(x)
  }

  rel <- merge(info$referential_constraints,
               info$key_column_usage,
               by = c("constraint_catalog",
                      "constraint_schema",
                      "constraint_name"))

  names(rel) <- paste0("fk_", names(rel))

  rel <- merge(rel,
               info$key_column_usage,
               by.x = c("fk_unique_constraint_catalog",
                        "fk_unique_constraint_schema",
                        "fk_unique_constraint_name",
                        "fk_ordinal_position"),
               by.y = c("constraint_catalog",
                        "constraint_schema",
                        "constraint_name",
                        "ordinal_position"))

  rel <- rel[, list(fk_constraint_name,
                    fk_table_catalog,
                    fk_table_schema,
                    fk_table_name,
                    fk_column_name,
                    pk_table_catalog = table_catalog,
                    pk_table_schema = table_schema,
                    pk_table_name = table_name,
                    pk_column_name = column_name)]

  ids <- unique(get_data_source(x)$id)
  ids <- sapply(ids, function(u) u@name)
  ids <- as.data.table(t(ids))

  setnames(ids, c("fk_table_catalog", "fk_table_schema", "fk_table_name"))
  rel <- as.data.table(rel[ids, on = names(ids)])

  setnames(ids, c("pk_table_catalog", "pk_table_schema", "pk_table_name"))
  rel <- rel[!ids, on = names(ids)]

  ids <- unique(rel[, list(table_catalog = pk_table_catalog,
                           table_schema = pk_table_schema,
                           table_name = pk_table_name)])
  columns <- as.data.table(info$columns[ids, list(catalog = table_catalog,
                                                  schema = table_schema,
                                                  table = table_name,
                                                  field = column_name,
                                                  position = ordinal_position),
                                        on = names(ids)])

  a <- attributes(x)
  data_source <- as.data.table(a$data_source)
  data_source[, catalog := vapply(id, function(u) u@name[["catalog"]], "")]
  data_source[, schema := vapply(id, function(u) u@name[["schema"]], "")]
  data_source[, table := vapply(id, function(u) u@name[["table"]], "")]

  fields <- as.data.table(a$fields)
  fields <- fields[data_source,
                   list(internal_name,
                        id_name,
                        catalog,
                        schema,
                        table,
                        field),
                   on = "id_name"]  

  x <- c(x)

  for (cnstr in unique(rel$fk_constraint_name)) {
    tmp <- rel[fk_constraint_name == cnstr]

    fk <- tmp[, list(id_name = fk_table_name, field = fk_column_name)]
    fk <- fields[fk, on = names(fk)]

    pk <- tmp[, list(catalog = pk_table_catalog,
                     schema = pk_table_schema,
                     table = pk_table_name,
                     field = pk_column_name)]

    if (nrow(merge(fields, pk, by = c("catalog", "schema", "table"))) == 0L) {
      n <- nrow(fields)
      new_id_name <- pk$table[[1L]]
      if (new_id_name %chin% fields$id_name) {
        new_id_name <- unique_table_name()
      }

      #' @importFrom DBI Id
      new_id <- Id(unlist(pk[1, list(catalog, schema, table)]))

      new_fields <- columns[pk[, c("catalog", "schema", "table")],
                            on = c("catalog", "schema", "table")]
      new_fields <- new_fields[order(position)]
      new_fields[, position := NULL]

      n <- nrow(fields)
      new_fields[, internal_name := paste0(session$key_base, .I + n)]
      new_fields[, id_name := new_id_name]
      setcolorder(new_fields, names(fields))

      pk <- new_fields[pk, on = names(pk)]

      on <- paste(pk$internal_name, fk$internal_name, sep = " == ")
      on <- handy_andy(as.list(parse(text = on)))

      new_ds <- pk[1, list(clause = "left outer join",
                           id = I(list(new_id)),
                           id_name = new_id_name,
                           on = I(list(on)),
                           catalog,
                           schema,
                           table)]

      new_x <- names_list(new_fields$internal_name)
      names(new_x) <- paste(new_id_name, new_fields$field, sep = ".")

      data_source <- rbind(data_source, new_ds)
      fields <- rbind(fields, new_fields)
      x <- c(x, new_x)
    }
  }

  a$data_source <- data_source[, c("clause", "id", "id_name", "on")]
  a$fields <- fields[, c("internal_name", "id_name", "field")]
  a$names <- names(x)
  
  x <- unname(x)
  attributes(x) <- a

  x
}



fk_column_name <- NULL
fk_constraint_name <- NULL
fk_table_catalog <- NULL
fk_table_name <- NULL
fk_table_schema <- NULL
pk_column_name <- NULL
pk_table_catalog <- NULL
pk_table_name <- NULL
pk_table_schema <- NULL

catalog <- NULL
schema <- NULL
field <- NULL
id <- NULL
id_name <- NULL
internal_name <- NULL
position <- NULL

catalog_x <- NULL
schema_x <- NULL
table_x <- NULL
