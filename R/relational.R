related_tables <- function(x, y = NULL) {
  info <- get_information_schema(x)
  if (is.null(info$referential_constraints) || is.null(info$key_column_usage)) {
    return(data.table())
  }

  r <- merge(info$referential_constraints, info$key_column_usage,
             by = c("constraint_catalog",
                    "constraint_schema",
                    "constraint_name"))

  names(r) <- paste0("fk_", names(r))

  r <- merge(r, info$key_column_usage,
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



match_fields <- function(x, fields) {
  table_fields <- get_fields(x)
  idx <- match(fields, table_fields$field)
  i_names <- lapply(table_fields$internal_name[idx], as.name)
  idx <- match(i_names, c(x))
  names(x)[idx]
}



relational_merge <- function(x, recursive = FALSE) {
  if (nrow(rt <- related_tables(x)) == 0L) {
    return(x)
  }

  info <- get_information_schema(x)
  columns <- info$columns[, c("table_catalog",
                              "table_schema",
                              "table_name",
                              "column_name",
                              "ordinal_position")]
  names(columns) <- c("catalog", "schema", "table", "field", "position")

  columns <- as.data.table(columns[catalog %in% rt$catalog_y &
                                     schema %in% rt$schema_y &
                                     table %in% rt$table_y])

  columns <- columns[rt[, list(catalog = catalog_y,
                               schema = schema_y,
                               table = table_y)],
                     on = c("catalog", "schema", "table")]

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

  for (cnstr in unique(rt$constraint)) {
    tmp <- rt[constraint == cnstr]

    fk <- tmp[, list(id_name = table_x, field = field_x)]
    fk <- fields[fk, on = names(fk)]

    pk <- tmp[, list(catalog = catalog_y,
                     schema = schema_y,
                     table = table_y,
                     field = field_y)]

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

    new_ds <- pk[1, list(clause = "LEFT OUTER JOIN",
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

    which_fk <- match(fk$internal_name, x)
    has_fk <- !is.na(which_fk)

    if (length(which_fk <- which_fk[has_fk])) {
      x <- c(x[which_fk], x[-which_fk])
    }

    if (length(drop_pk <- match(pk[has_fk == TRUE]$internal_name, new_x))) {
      new_x <- new_x[-drop_pk]
    }

    x <- c(x, new_x)
  }

  a$data_source <- setDF(data_source[, c("clause", "id", "id_name", "on")])
  a$fields <- setDF(fields[, c("internal_name", "id_name", "field")])
  a$names <- names(x)

  x <- unname(x)
  attributes(x) <- a

  if (recursive) {
    x <- relational_merge(x, recursive = TRUE)
  }

  x
}



catalog <- NULL
catalog_x <- NULL
catalog_y <- NULL
constraint <- NULL
field <- NULL
field_x <- NULL
field_y <- NULL
fk_column_name <- NULL
fk_constraint_name <- NULL
fk_table_catalog <- NULL
fk_table_name <- NULL
fk_table_schema <- NULL
id <- NULL
id_name <- NULL
internal_name <- NULL
position <- NULL
schema <- NULL
schema_x <- NULL
schema_y <- NULL
table_x <- NULL
table_y <- NULL
