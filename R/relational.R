related_tables <- function(x, y = NULL) {
  UseMethod("related_tables", dbi_connection(x))
}



#' @rawNamespace S3method(related_tables,default,related_tables_default)
related_tables_default <- function(x, y = NULL) {
  fks <- foreign_keys(get_catalog(x))

  dots <- list(table_catalog = fks$fk_table_catalog,
               table_schema = fks$fk_table_schema,
               table_name = fks$fk_table_name)
  dots <- dots[vapply(dots, length, 0L) > 0L]
  fks$fk_id <- .mapply(DBI::Id, dots, NULL)

  dots <- list(table_catalog = fks$pk_table_catalog,
               table_schema = fks$pk_table_schema,
               table_name = fks$pk_table_name)
  dots <- dots[vapply(dots, length, 0L) > 0L]
  fks$pk_id <- .mapply(DBI::Id, dots, NULL)

  fks <- split(fks, fks$constraint_name)
  names(fks) <- NULL

  fks <- lapply(fks, function(u) {
    data.frame(fk_id = I(u$fk_id[1L]),
               fk_columns = I(list(u$fk_column_name[u$seq])),
               pk_id = I(u$pk_id[1L]),
               pk_columns = I(list(u$pk_column_name[u$seq])))
  })

  fks <- do.call(rbind, fks)

  names_out <- c(y_id = "x_id", y_columns = "x_columns",
                 x_id = "y_id", x_columns = "y_columns")

  names(fks) <- names_out

  x_ids <- unique(get_data_source(x)[, "id", drop = FALSE])
  x_fks <- merge(fks, x_ids, by.x = "x_id", by.y = "id", sort = FALSE)

  if (!is.null(y)) {
    y_ids <- unique(get_data_source(y)[, "id", drop = FALSE])
    x_fks <- merge(x_fks, y_ids, by.x = "y_id", by.y = "id", sort = FALSE)

    y_fks <- merge(fks, y_ids, by.x = "x_id", by.y = "id", sort = FALSE)
    y_fks <- merge(y_fks, x_ids, by.x = "y_id", by.y = "id", sort = FALSE)

    names(y_fks) <- names_out[names(y_fks)]
    y_fks <- y_fks[, names_out]

    x_fks <- rbind(x_fks[, names_out], y_fks[, names_out])
  }

  x_fks[, names_out]
}



#' @rawNamespace S3method(related_tables,SQLiteConnection,related_tables_sqlite)
related_tables_sqlite <- function(x, y = NULL) {
  x_id <- get_data_source(x)$id
  table_name <- dbQuoteString(x, x_id[[1L]]@name[["table_name"]])
  statement <- sprintf(sql_statement("related_tables_sqlite"), table_name)
  fkl <- DBI:::dbGetQuery(x, statement)

  if ((m <- nrow(fkl)) > 0L) {
    dots <- list(table_schema = "main", table_name = fkl$table)
    fkl$pk_id <- I(.mapply(DBI::Id, dots, NULL))
    fkl$fk_id <- I(rep(x_id, m))

    fkl <- split(fkl, fkl$id)
    names(fkl) <- NULL

    fkl <- lapply(fkl, function(u) {
      data.frame(x_id = I(u$fk_id[1L]),
                 x_columns = I(list(u$from[u$seq])),
                 y_id = I(u$pk_id[1L]),
                 y_columns = I(list(u$to[u$seq])))
    })

    fkl <- do.call(rbind, fkl)
  }

  if (length(y)) {
    if (nrow(fkl)) {
      y_id <- get_data_source(y)$id
      fkl <- fkl[match(y_id, fkl$y_id, nomatch = 0L), ]
    }

    if (!nrow(fkl)) {
      fkl <- related_tables_sqlite(y)

      if (length(fkl)) {
        fkl <- fkl[match(x_id, fkl$y_id, nomatch = 0L), ]
        fkl_names <- names(fkl)
        fkl <- fkl[, c("y_id", "y_columns", "x_id", "x_columns")]
        names(fkl) <- fkl_names
      }
    }
  }

  if (length(fkl) && !nrow(fkl)) {
    fkl <- NULL
  }

  fkl
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



foreign_keys <- function(catalog) {
  if (!is_dbi_catalog(catalog)) {
    return(NULL)
  }

  UseMethod("foreign_keys", dbi_connection(catalog))
}



#' @rawNamespace S3method(foreign_keys,default,foreign_keys_default)
foreign_keys_default <- function(catalog) {
  NULL
}



#' @rawNamespace S3method(foreign_keys,duckdb_connection,foreign_keys_duckdb)
foreign_keys_duckdb <- function(catalog) {
  info <- get_catalog(catalog)$information_schema

  referential_constraints <- info$referential_constraints
  key_column_usage <- info$key_column_usage

  r <- merge(referential_constraints, key_column_usage,
             by = c("constraint_catalog",
                    "constraint_schema",
                    "constraint_name"))

  names(r) <- paste0("fk_", names(r))

  r <- merge(key_column_usage, r,
             by.x = c("constraint_catalog",
                      "constraint_schema",
                      "constraint_name",
                      "ordinal_position"),
             by.y = c("fk_unique_constraint_catalog",
                      "fk_unique_constraint_schema",
                      "fk_unique_constraint_name",
                      "fk_ordinal_position"))

  r <- r[, list(constraint_name = fk_constraint_name,
                fk_table_catalog = fk_table_catalog,
                fk_table_schema = fk_table_schema,
                fk_table_name = fk_table_name,
                fk_column_name = fk_column_name,
                pk_table_catalog = table_catalog,
                pk_table_schema = table_schema,
                pk_table_name = table_name,
                pk_column_name = column_name)]

  as.data.frame(r, n = -1L)
}
