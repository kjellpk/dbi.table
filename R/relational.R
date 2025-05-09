related_tables <- function(x, y = NULL) {
  if (!is_pristine(x) || (length(y) && !is_pristine(y))) {
    return(NULL)
  }

  rt <- foreign_keys(x)

  if (length(y)) {
    y_id <- get_data_source(y)[, "id", drop = FALSE]

    if (length(rt)) {
      rt <- merge(rt, y_id, by.x = "y_id", by.y = "id", sort = FALSE)
      if (nrow(rt)) {
        return(rt[, c("x_id", "x_columns", "y_id", "y_columns")])
      }
    }

    if (length(rt <- foreign_keys(y))) {
      x_id <- get_data_source(x)[, "id", drop = FALSE]
      rt <- merge(rt, x_id, by.x = "y_id", by.y = "id", sort = FALSE)

      if (nrow(rt)) {
        rt <- rt[, c("y_id", "y_columns", "x_id", "x_columns")]
        names(rt) <- c("x_id", "x_columns", "y_id", "y_columns")
      } else {
        rt <- NULL
      }
    }
  }

  rt
}



relational_merge <- function(x, recursive = FALSE, ids = list()) {
  if (is.null(rt <- related_tables(x))) {
    return(x)
  }

  if (nrow(rt <- rt[!(rt$y_id %in% ids), , drop = FALSE]) == 0L) {
    return(x)
  }

  catalog <- get_catalog(x)

  for (i in seq_len(nrow(rt))) {
    y_id <- rt[[i, "y_id"]]
    if (is.na(schema <- unname(y_id@name["table_schema"]))) {
      schema <- "main"
    }
    name <- y_id@name[["table_name"]]

    y <- catalog[[schema]][[name]]

    if (recursive) {
      ids <- c(ids, unique(get_data_source(x)$id))
      y <- relational_merge(y, TRUE, ids)
    }

    by_x <- rt[[i, "x_columns"]]

    if (length(by_y <- rt[[i, "y_columns"]]) == 1L) {
      prefix <- paste0(by_x, ".")
    } else {
      prefix <- paste0(name, ".")
    }

    names(y) <- paste0(prefix, names(y))
    by_y <- paste0(prefix, by_y)

    x <- merge(x, y, by.x = by_x, by.y = by_y, all.x = TRUE)
  }

  x
}



foreign_keys <- function(x) {
  if (is.null(catalog <- get_catalog(x))) {
    return(NULL)
  }

  id <- get_data_source(x)[[1L, "id"]]

  if (is.null(fk <- get0("./foreign_keys", catalog, inherits = FALSE))) {
    if (is.null(fk <- foreign_keys_(catalog, id))) {
      return(NULL)
    }

    idx <- vapply(fk, bit64::is.integer64, FALSE)
    fk[, idx] <- lapply(fk[, idx,  drop = FALSE], as.integer)

    dots <- list(table_catalog = fk$fk_table_catalog,
                 table_schema = fk$fk_table_schema,
                 table_name = fk$fk_table_name)
    dots <- dots[vapply(dots, length, 0L) > 0L]
    fk$fk_id <- .mapply(DBI::Id, dots, NULL)

    dots <- list(table_catalog = fk$pk_table_catalog,
                 table_schema = fk$pk_table_schema,
                 table_name = fk$pk_table_name)
    dots <- dots[vapply(dots, length, 0L) > 0L]
    fk$pk_id <- .mapply(DBI::Id, dots, NULL)

    fk <- split(fk, fk$constraint_name)
    names(fk) <- NULL

    fk <- lapply(fk, function(u) {
      data.frame(x_id = I(u$fk_id[1L]),
                 x_columns = I(list(u$fk_column_name[u$key_ordinal_position])),
                 y_id = I(u$pk_id[1L]),
                 y_columns = I(list(u$pk_column_name[u$key_ordinal_position])))
    })

    fk <- do.call(rbind, fk)

    if (length(unique(fk$x_id)) > 1L || !identical(id, fk[[1L, "x_id"]])) {
      assign("./foreign_keys", fk, pos = catalog)
    }
  }

  fk <- merge(fk, data.frame(x_id = I(list(id))), by = "x_id", sort = FALSE)
  if (nrow(fk)) fk else NULL
}



foreign_keys_ <- function(catalog, id) {
  UseMethod("foreign_keys_", dbi_connection(catalog))
}



#' @rawNamespace S3method(foreign_keys_,default,foreign_keys_default)
foreign_keys_default <- function(catalog, id) {
  NULL
}
