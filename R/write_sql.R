write_select_query <- function(x, n = -1L, strict = FALSE) {
  if (inherits(x, "SQL")) {
    return(x)
  }

  query <- list(ctes = write_ctes(x),
                select = write_select(x, n),
                from = write_from(x),
                where = write_where(x),
                group_by = write_group_by(x),
                order_by = write_order_by(x, strict),
                limit = write_limit(x, n))

  paste(query[!sapply(query, is.null)], collapse = "\n\n")
}



write_ctes <- function(x) {
  if (length(ctes <- get_ctes(x))) {
    ctes <- lapply(ctes, write_select_query, n = -1L)
    ctes <- paste0(DBI::dbQuoteIdentifier(x, names(ctes)), " AS (\n", ctes)
    ctes <- paste(ctes, collapse = "\n),\n\n")
    paste0("WITH ", ctes, "\n)")
  } else {
    NULL
  }
}



write_select <- function(x, n) {
  UseMethod("write_select", dbi_connection(x))
}



#' @rawNamespace S3method(write_select,default,write_select_default)
write_select_default <- function(x, n) {
  if (all(vapply(setdiff(c(x), get_group_by(x)), call_can_aggregate, FALSE))) {
    select <- translate_sql_(c(x), con = x, window = FALSE)
  } else {
    select <- list()
    for (i in seq_along(x)) {
      if (!is.null(over <- attr(x[[i]], "over", exact = TRUE))) {
        pb <- translate_sql_(over$partition_by, con = x, window = FALSE)
        ob <- translate_sql_(over$order_by, con = x, window = FALSE)
      } else {
        pb <- ob <- NULL
      }

      select[[i]] <- translate_sql_(unname(c(x)[i]), con = x,
                                    vars_group = pb, vars_order = ob)
    }
  }

  select <- sub_db_identifier(unlist(select), x, get_fields(x))
  select <- paste(select, "AS", DBI::dbQuoteIdentifier(x, names(x)))
  pad1 <- ifelse(get_distinct(x), "SELECT DISTINCT", "SELECT")

  pad <- rep(ws(nchar(pad1)), length(x))
  pad[1] <- pad1

  paste(paste(pad, select), collapse = ",\n")
}



write_from <- function(x) {
  UseMethod("write_from", dbi_connection(x))
}



#' @rawNamespace S3method(write_from,default,write_from_default)
write_from_default <- function(x) {
  from <- ""

  for (i in seq_len(nrow(data_source <- get_data_source(x)))) {
    from <- paste0(from,
                   pad_left(data_source[i, "clause"]),
                   " ",
                   DBI::dbQuoteIdentifier(x, data_source[[i, "id"]]),
                   " AS ",
                   DBI::dbQuoteIdentifier(x, data_source[[i, "id_name"]]))

    if (!is.null(on <- data_source[[i, "on"]])) {
      on <- translate_sql_(list(on), con = x, window = FALSE)

      from <- paste0(from,
                     "\n",
                     pad_left("ON"),
                     " ",
                     sub_db_identifier(on, x, get_fields(x)))
    }

    if (i < nrow(data_source)) {
      from <- paste0(from, "\n\n")
    }
  }

  from
}



write_where <- function(x) {
  UseMethod("write_where", dbi_connection(x))
}



#' @rawNamespace S3method(write_where,default,write_where_default)
write_where_default <- function(x) {
  if (length(where <- get_where(x))) {
    where <- translate_sql_(list(handy_andy(where)), con = x, window = FALSE)
    where <- sub_db_identifier(where, x, get_fields(x))
    paste(pad_left("WHERE"), where)
  } else {
    NULL
  }
}



write_group_by <- function(x) {
  UseMethod("write_group_by", dbi_connection(x))
}



#' @rawNamespace S3method(write_group_by,default,write_group_by_default)
write_group_by_default <- function(x) {
  if (length(group_by <- get_group_by(x))) {
    group_by <- translate_sql_(c(group_by), con = x, window = FALSE)
    group_by <- sub_db_identifier(group_by, x, get_fields(x))
    paste(pad_left("GROUP BY"), paste(group_by, collapse = ", "))
  } else {
    NULL
  }
}



write_order_by <- function(x, strict) {
  UseMethod("write_order_by", dbi_connection(x))
}



#' @rawNamespace S3method(write_order_by,default,write_order_by_default)
write_order_by_default <- function(x, strict) {
  if (is.null(order_by <- get_order_by(x)) && strict) {
    x_key <- sub_lang(lapply(get_key(x), as.name), c(x), NULL)
    order_by <- unique(c(order_by, x_key))
  }

  if (length(order_by)) {
    is_unary_minus <- function(u) {
      is.call(u) && (u[[1]] == as.name("-")) && (length(u) == 2)
    }

    desc <- vapply(order_by, is_unary_minus, FALSE)
    order_by[desc] <- lapply(order_by[desc], `[[`, i = 2)

    order_by <- translate_sql_(order_by, con = x, window = FALSE)
    order_by <- sub_db_identifier(order_by, x, get_fields(x))
    order_by[desc] <- paste(order_by[desc], "DESC")
    paste(pad_left("ORDER BY"), paste(order_by, collapse = ", "))
  } else {
    NULL
  }
}



write_limit <- function(x, n) {
  UseMethod("write_limit", dbi_connection(x))
}



#' @rawNamespace S3method(write_limit,default,write_limit_default)
write_limit_default <- function(x, n) {
  if (n >= 0L) paste(" LIMIT", n) else NULL
}
