write_select_query <- function(x) {
  query <- list(ctes = write_ctes(x),
                select = write_select(x),
                from = write_from(x),
                where = write_where(x),
                group_by = write_group_by(x),
                order_by = write_order_by(x))

  paste(query[!sapply(query, is.null)], collapse = "\n\n")
}



write_ctes <- function(x) {
  if (length(ctes <- get_ctes(x))) {
    ctes <- lapply(ctes, write_select_query)
    ctes <- paste0(dbQuoteIdentifier(get_connection(x), names(ctes)),
                   " AS (\n", ctes)
    ctes <- paste(ctes, collapse = "\n),\n\n")
    paste0("WITH ", ctes, "\n)")
  } else {
    NULL
  }
}



write_select <- function(x) {
  conn <- get_connection(x)

  if (all(vapply(setdiff(c(x), get_group_by(x)), can_aggregate, FALSE))) {
    ## @importFrom dbplyr translate_sql_
    select <- translate_sql_(c(x), con = conn, window = FALSE)
  } else {
    select <- list()
    for (i in seq_along(x)) {
      if (!is.null(over <- attr(x[[i]], "over", exact = TRUE))) {
        ## @importFrom dbplyr translate_sql_
        pb <- translate_sql_(over$partition_by, con = conn, window = FALSE)
        ob <- translate_sql_(over$order_by, con = conn, window = FALSE)
      } else {
        pb <- ob <- NULL
      }

      ## @importFrom dbplyr translate_sql_
      select[[i]] <- translate_sql_(unname(c(x)[i]), con = conn,
                                    vars_group = pb, vars_order = ob)
    }
  }

  select <- sub_db_identifier(unlist(select), conn, get_fields(x))

  #' @importFrom DBI dbQuoteIdentifier
  select <- paste(select, "AS", dbQuoteIdentifier(conn, names(x)))

  pad1 <- ifelse(get_distinct(x), "SELECT DISTINCT", "SELECT")
  pad <- rep(ws(nchar(pad1)), length(x))
  pad[1] <- pad1
  paste(paste(pad, select), collapse = ",\n")
}



write_from <- function(x) {
  conn <- get_connection(x)
  from <- ""

  for (i in seq_len(nrow(data_source <- get_data_source(x)))) {
    from <- paste0(from,
                   pad_left(data_source[i, "clause"]),
                   " ",
                   dbQuoteIdentifier(conn, data_source[[i, "id"]]),
                   " AS ",
                   dbQuoteIdentifier(conn, data_source[[i, "id_name"]]))

    if (!is.null(on <- data_source[[i, "on"]])) {
      ## @importFrom dbplyr translate_sql_
      on <- translate_sql_(list(on), con = conn, window = FALSE)

      from <- paste0(from,
                     "\n",
                     pad_left("ON"),
                     " ",
                     sub_db_identifier(on, conn, get_fields(x)))
    }

    if (i < nrow(data_source)) {
      from <- paste0(from, "\n\n")
    }
  }

  from
}



write_where <- function(x) {
  conn <- get_connection(x)

  if (length(where <- get_where(x))) {
    ## @importFrom dbplyr translate_sql_
    where <- translate_sql_(list(handy_andy(where)), con = conn, window = FALSE)
    where <- sub_db_identifier(where, conn, get_fields(x))
    paste(pad_left("WHERE"), where)
  } else {
    NULL
  }
}



write_group_by <- function(x) {
  conn <- get_connection(x)

  if (length(group_by <- get_group_by(x))) {
    ## @importFrom dbplyr translate_sql_
    group_by <- translate_sql_(c(group_by), con = conn, window = FALSE)
    group_by <- sub_db_identifier(group_by, conn, get_fields(x))
    paste(pad_left("GROUP BY"), paste(group_by, collapse = ", "))
  } else {
    NULL
  }
}



write_order_by <- function(x) {
  conn <- get_connection(x)

  if (length(order_by <- get_order_by(x))) {

    is_unary_minus <- function(u) {
      is.call(u) && (u[[1]] == as.name("-")) && (length(u) == 2)
    }

    desc <- vapply(order_by, is_unary_minus, FALSE)
    order_by[desc] <- lapply(order_by[desc], `[[`, i = 2)

    ## @importFrom dbplyr translate_sql_
    order_by <- translate_sql_(order_by, con = conn, window = FALSE)
    order_by <- sub_db_identifier(order_by, conn, get_fields(x))
    order_by[desc] <- paste(order_by[desc], "DESC")
    paste(pad_left("ORDER BY"), paste(order_by, collapse = ", "))
  } else {
    NULL
  }
}
