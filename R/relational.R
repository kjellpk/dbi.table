FIRST_MERGE_BY <- c("constraint_catalog",
                    "constraint_schema",
                    "constraint_name",
                    "table_name",
                    "referenced_table_name")



SECOND_MERGE_BY <- c(fk_unique_constraint_catalog = "constraint_catalog",
                     fk_unique_constraint_schema = "constraint_schema",
                     fk_unique_constraint_name = "constraint_name",
                     fk_referenced_table_name = "table_name",
                     fk_ordinal_position = "ordinal_position")



related_tables <- function(x, y = NULL) {
  info <- get_information_schema(x)

  if (is.null(referential_constraints <- info$referential_constraints) ||
        is.null(key_column_usage <- info$key_column_usage)) {
    return(NULL)
  }

  merge_by <- intersect(FIRST_MERGE_BY, names(referential_constraints))

  r <- merge(referential_constraints, key_column_usage, by = merge_by)

  names(r) <- paste0("fk_", names(r))

  merge_by <- intersect(names(SECOND_MERGE_BY), names(r))
  merge_by <- SECOND_MERGE_BY[merge_by]

  r <- merge(key_column_usage, r, by.x = merge_by, by.y = names(merge_by))

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
  xids <- as.data.frame(t(vapply(xids, function(u) u@name, character(3L))))
  xids <- as.dbi.table(info, xids, type = "query")
  names(xids) <- c("catalog_x", "schema_x", "table_x")

  rx <- r[xids, nomatch = NULL, on = names(xids)]

  names(xids) <- c("catalog_y", "schema_y", "table_y")
  rx <- rx[!xids, nomatch = NULL, on = names(xids)]

  if (!is.null(y)) {
    yids <- unique(get_data_source(y)$id)
    yids <- as.data.frame(t(vapply(yids, function(u) u@name, character(3L))))
    yids <- as.dbi.table(info, yids, type = "query")
    names(yids) <- c("catalog_y", "schema_y", "table_y")

    rx <- rx[yids, nomatch = NULL, on = names(yids)]

    names(yids) <- c("catalog_x", "schema_x", "table_x")
    ry <- r[yids, nomatch = NULL, on = names(yids)]
    ry <- ry[xids, nomatch = NULL, on = names(xids)]

    names(ry) <- c("constraint",
                   "catalog_y", "schema_y", "table_y", "field_y",
                   "catalog_x", "schema_x", "table_x", "field_x")

    rx <- as.data.frame(rx)
    ry <- as.data.frame(ry)[, names(rx)]
    rbind(rx, ry)
  } else {
    as.data.frame(rx)
  }
}



match_by_field <- function(x, fields) {
  table_fields <- get_fields(x)

  if (nrow(id <- unique(fields[, 1:3])) != 1L) {
    return(rep(NA_character_, nrow(fields)))
  }

  id_name <- id[[1L, 3L]]
  table_fields <- table_fields[table_fields$id_name == id_name, ]
  idx <- match(fields[, 4L], table_fields$field)

  internal_names <- table_fields[idx, "internal_name"]
  internal_names <- lapply(internal_names, as.name)

  cx <- c(x)
  cx <- cx[vapply(cx, is.name, FALSE)]
  cx <- cx[!duplicated(cx)]

  idx <- match(internal_names, cx)
  names(cx[idx])
}



relational_merge <- function(x, recursive = FALSE) {
  if (is.null(rt <- related_tables(x)) || nrow(rt) == 0L) {
    return(x)
  }

  info <- get_information_schema(x)
  columns <- info$columns[, c("table_catalog",
                              "table_schema",
                              "table_name",
                              "column_name",
                              "ordinal_position")]

  uni_rt <- unique(rt[, c("catalog_y", "schema_y", "table_y")])
  names(uni_rt) <- c("table_catalog", "table_schema", "table_name")

  columns <- columns[uni_rt, c("table_catalog",
                               "table_schema",
                               "table_name",
                               "column_name",
                               "ordinal_position"),
                     nomatch = NULL,
                     on = c("table_catalog", "table_schema", "table_name")]
  columns <- as.data.frame(columns)

  data_source <- get_data_source(x)
  merge_source <- data_source[!duplicated(data_source$id), c("id_name", "id")]
  merge_source <- cbind(merge_source,
                        t(sapply(merge_source$id, function(u) u@name)))

  fields <- get_fields(x)
  merge_fields <- merge(fields, merge_source, by = "id_name")

  a <- attributes(x)
  x <- c(x)

  for (cnstr in unique(rt$constraint)) {
    tmp <- rt[rt$constraint == cnstr, ]

    fk <- tmp[, c("catalog_x", "schema_x", "table_x", "field_x")]
    names(fk) <- c("table_catalog", "table_schema", "table_name", "field")

    fk <- merge(merge_fields, fk, by = names(fk))

    pk <- tmp[, c("catalog_y", "schema_y", "table_y", "field_y")]
    names(pk) <- c("table_catalog", "table_schema", "table_name", "field")

    new_id_name <- pk$table_name[[1L]]
    if (new_id_name %in% fields$id_name) {
      new_id_name <- unique_table_name()
    }

    new_id <- pk[1L, c("table_catalog", "table_schema", "table_name")]
    new_id <- DBI::Id(unlist(new_id))

    new_fields <- merge(columns,
                        pk[, c("table_catalog", "table_schema", "table_name")],
                        by = c("table_catalog",
                               "table_schema",
                               "table_name"))

    names(new_fields)[names(new_fields) == "column_name"] <- "field"
    new_fields <- new_fields[order(new_fields$ordinal_position), ]

    n <- nrow(fields)
    new_fields$internal_name <- paste0(session$key_base,
                                       seq_len(nrow(new_fields)) + n)
    new_fields$id_name <- new_id_name

    pk <- merge(new_fields, pk, by = names(pk))

    on <- paste(pk$internal_name, fk$internal_name, sep = " == ")
    on <- handy_andy(as.list(parse(text = on)))

    new_ds <- data.frame(clause = "LEFT OUTER JOIN",
                         id = I(list(new_id)),
                         id_name = new_id_name,
                         on = I(list(on)))

    new_x <- names_list(new_fields$internal_name)
    names(new_x) <- paste(new_id_name, new_fields$field, sep = ".")

    data_source <- rbind(data_source, new_ds)
    fields <- rbind(fields, new_fields[, names(fields)])

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

  a$data_source <- data_source
  a$fields <- fields
  a$names <- names(x)

  x <- unname(x)
  attributes(x) <- a

  if (recursive) {
    x <- relational_merge(x, recursive = TRUE)
  }

  x
}



fk_constraint_name <- NULL
fk_table_catalog <- NULL
fk_table_schema <- NULL
fk_table_name <- NULL
fk_column_name <- NULL
table_catalog <- NULL
table_schema <- NULL
table_name <- NULL
column_name <- NULL
