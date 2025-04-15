related_tables <- function(x, y = NULL) {
  UseMethod("related_tables")
}



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


#' @rawNamespace S3method(related_tables,default,related_tables_default)
related_tables_default <- function(x, y = NULL) {
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

  rt <- split(rt, rt$constraint)

  for (i in seq_along(rt)) {
    rt_x <- rt[[i]][, c("catalog_x", "schema_x", "table_x", "field_x")]
    if (!anyNA(by_x <- match_by_field(x, rt_x))) {
      rt_y <- rt[[i]][, c("catalog_y", "schema_y", "table_y", "field_y")]
      names(rt_y) <- c("table_catalog", "table_schema", "table_name", "field_y")
      by_y <- paste(rt_y$table_name, rt_y$field_y, sep = ".")

      rt_id <- rt_y[1L, c("table_catalog", "table_schema", "table_name")]
      y_id <- DBI::Id(unlist(rt_id))

      y_fields <- merge(columns, rt_id)
      y_fields <- y_fields$column_name[order(y_fields$ordinal_position)]

      y <- new_dbi_table(get_connection(x), y_id, y_fields)
      names(y) <- paste(rt_y$table_name, names(y), sep = ".")

      x <- merge(x, y, by.x = by_x, by.y = by_y, all.x = TRUE)
    }
  }

  if (recursive) {
    return(relational_merge(x, recursive))
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
