related_tables <- function(x, y = NULL) {
  info <- get_information_schema(x)
  if (is.null(info$REFERENTIAL_CONSTRAINTS) || is.null(info$KEY_COLUMN_USAGE)) {
    return(NULL)
  }

  r <- merge(info$REFERENTIAL_CONSTRAINTS,
             info$KEY_COLUMN_USAGE,
             by = c("CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME"))

  names(r) <- paste0("FK_", names(r))

  r <- merge(r,
             info$KEY_COLUMN_USAGE,
             by.x = c("FK_UNIQUE_CONSTRAINT_CATALOG",
                      "FK_UNIQUE_CONSTRAINT_SCHEMA",
                      "FK_UNIQUE_CONSTRAINT_NAME",
                      "FK_ORDINAL_POSITION"),
             by.y = c("CONSTRAINT_CATALOG",
                      "CONSTRAINT_SCHEMA",
                      "CONSTRAINT_NAME",
                      "ORDINAL_POSITION"))

  r <- r[, list(constraint = FK_CONSTRAINT_NAME,
                catalog_x = FK_TABLE_CATALOG,
                schema_x = FK_TABLE_SCHEMA,
                table_x = FK_TABLE_NAME,
                field_x = FK_COLUMN_NAME,
                catalog_y = TABLE_CATALOG,
                schema_y = TABLE_SCHEMA,
                table_y = TABLE_NAME,
                field_y = COLUMN_NAME)]

  xids <- unique(get_data_source(x)$id)
  xids <- as.data.table(t(sapply(xids, function(u) u@name)))

  setnames(xids, c("catalog_x", "schema_x", "table_x"))
  dbi_xids <- as.dbi.table(xids, get_connection(r))
  rx <- as.data.table(r[dbi_xids, nomatch = NULL, on = names(dbi_xids)])

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

  if (is.null(info$REFERENTIAL_CONSTRAINTS) || is.null(info$KEY_COLUMN_USAGE)) {
    return(x)
  }

  rel <- merge(info$REFERENTIAL_CONSTRAINTS,
               info$KEY_COLUMN_USAGE,
               by = c("CONSTRAINT_CATALOG",
                      "CONSTRAINT_SCHEMA",
                      "CONSTRAINT_NAME"))

  names(rel) <- paste0("FK_", names(rel))

  rel <- merge(rel,
               info$KEY_COLUMN_USAGE,
               by.x = c("FK_UNIQUE_CONSTRAINT_CATALOG",
                        "FK_UNIQUE_CONSTRAINT_SCHEMA",
                        "FK_UNIQUE_CONSTRAINT_NAME",
                        "FK_ORDINAL_POSITION"),
               by.y = c("CONSTRAINT_CATALOG",
                        "CONSTRAINT_SCHEMA",
                        "CONSTRAINT_NAME",
                        "ORDINAL_POSITION"))

  rel <- rel[, list(FK_CONSTRAINT_NAME,
                    FK_TABLE_CATALOG,
                    FK_TABLE_SCHEMA,
                    FK_TABLE_NAME,
                    FK_COLUMN_NAME,
                    PK_TABLE_CATALOG = TABLE_CATALOG,
                    PK_TABLE_SCHEMA = TABLE_SCHEMA,
                    PK_TABLE_NAME = TABLE_NAME,
                    PK_COLUMN_NAME = COLUMN_NAME)]

  ids <- unique(get_data_source(x)$id)
  ids <- sapply(ids, function(u) u@name)
  ids <- as.data.table(t(ids))

  setnames(ids, c("FK_TABLE_CATALOG", "FK_TABLE_SCHEMA", "FK_TABLE_NAME"))
  rel <- as.data.table(rel[ids, on = names(ids)])

  setnames(ids, c("PK_TABLE_CATALOG", "PK_TABLE_SCHEMA", "PK_TABLE_NAME"))
  rel <- rel[!ids, on = names(ids)]

  ids <- unique(rel[, list(TABLE_CATALOG = PK_TABLE_CATALOG,
                           TABLE_SCHEMA = PK_TABLE_SCHEMA,
                           TABLE_NAME = PK_TABLE_NAME)])
  columns <- as.data.table(info$COLUMNS[ids, list(catalog = TABLE_CATALOG,
                                                  schema = TABLE_SCHEMA,
                                                  table = TABLE_NAME,
                                                  field = COLUMN_NAME,
                                                  position = ORDINAL_POSITION),
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

  for (cnstr in unique(rel$FK_CONSTRAINT_NAME)) {
    tmp <- rel[FK_CONSTRAINT_NAME == cnstr]

    fk <- tmp[, list(id_name = FK_TABLE_NAME, field = FK_COLUMN_NAME)]
    fk <- fields[fk, on = names(fk)]

    pk <- tmp[, list(catalog = PK_TABLE_CATALOG,
                     schema = PK_TABLE_SCHEMA,
                     table = PK_TABLE_NAME,
                     field = PK_COLUMN_NAME)]

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



FK_COLUMN_NAME <- NULL
FK_CONSTRAINT_NAME <- NULL
FK_TABLE_CATALOG <- NULL
FK_TABLE_NAME <- NULL
FK_TABLE_SCHEMA <- NULL
PK_COLUMN_NAME <- NULL
PK_TABLE_CATALOG <- NULL
PK_TABLE_NAME <- NULL
PK_TABLE_SCHEMA <- NULL

catalog <- NULL
schema <- NULL
field <- NULL
id <- NULL
id_name <- NULL
internal_name <- NULL
position <- NULL
