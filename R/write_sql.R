write_sql <- function(x) {
  query <- list(ctes = write_ctes(x),
                select = write_select(x),
                from = write_from(x),
                where = write_where(x),
                by = write_group_by(x),
                order = write_order_by(x))

  paste(query[!sapply(query, is.null)], collapse = "\n\n")
}



write_ctes <- function(x) {
  a <- attributes(x)

  if (!length(ctes <- a$ctes)) {
    return(NULL)
  }

  ctes <- lapply(ctes, write_sql)
  ctes <- paste0(dbQuoteIdentifier(get_connection(x), names(ctes)),
                 " AS (\n", ctes)
  ctes <- paste(ctes, collapse = "\n),\n\n")
  paste0("WITH ", ctes, "\n)")
}



write_select <- function(x) {
  x_names <- names(x)
  conn <- get_connection(x)
  fields <- attr(x, "fields", exact = TRUE)
  distinct <- attr(x, "distinct", exact = TRUE)

  if (!length(attr(x, "by", exact = TRUE)) &&
      !all(vapply(x, can_aggregate, FALSE))) {

    for (i in seq_along(x)) {
      if (!is.null(over <- attr(x[[i]], "over", exact = TRUE))) {
        #' @importFrom dbplyr translate_sql_
        pb <- translate_sql_(over$partition_by, con = conn, window = FALSE)
        ob <- translate_sql_(over$order_by, con = conn, window = FALSE)
      } else {
        pb <- ob <- NULL
      }

      #' @importFrom dbplyr translate_sql_
      x[[i]] <- translate_sql_(unname(c(x)[i]), con = conn,
                               vars_group = pb, vars_order = ob)
    }

    x <- unlist(x)
  } else {
    #' @importFrom dbplyr translate_sql_
    x <- translate_sql_(c(x), con = conn, window = FALSE)
  }

  x <- sub_db_identifier(x, conn, fields)

  #' @importFrom DBI dbQuoteIdentifier
  x <- paste(x, "AS", dbQuoteIdentifier(conn, x_names))

  s1 <- ifelse(distinct, "SELECT DISTINCT", "SELECT")
  s <- rep(ws(nchar(s1)), length(x))
  s[1] <- s1
  paste(paste(s, x), collapse = ",\n")
}



write_from <- function(x) {
  a <- attributes(x)
  conn <- get_connection(x)
  from <- ""

  for (i in seq_len(nrow(ds <- a$data_source))) {
    from <- paste0(from,
                   pad_left(ds[i, "clause"]),
                   " ",
                   dbQuoteIdentifier(conn, ds[[i, "id"]]),
                   " AS ",
                   dbQuoteIdentifier(conn, ds[[i, "id_name"]]))

    if (!is.null(on <- ds[[i, "on"]])) {
       #' @importFrom dbplyr translate_sql_
      on <- translate_sql_(list(on), con = conn, window = FALSE)

      from <- paste0(from,
                     "\n",
                     pad_left("ON"),
                     " ",
                     sub_db_identifier(on, conn, a$fields))
    }

    if (i < nrow(ds)) {
      from <- paste0(from, "\n\n")
    }
  }

  from
}



write_where <- function(x) {
  a <- attributes(x)
  conn <- get_connection(x)

  if (!length(a$where))
    return(NULL)

  #' @importFrom dbplyr translate_sql_
  w <- translate_sql_(list(handy_andy(a$where)), con = conn, window = FALSE)
  w <- sub_db_identifier(w, conn, a$fields)

  paste(pad_left("WHERE"), w)
}



write_group_by <- function(x) {
  a <- attributes(x)
  conn <- get_connection(x)

  if (!length(a$by)) {
    return(NULL)
  }

  #' @importFrom dbplyr translate_sql_
  gb <- translate_sql_(a$by, con = conn, window = FALSE)
  gb <- sub_db_identifier(gb, conn, a$fields)

  paste(pad_left("GROUP BY"), paste(gb, collapse = ", "))
}



write_order_by <- function(x) {
  conn <- get_connection(x)
  fields <- attr(x, "fields", exact = TRUE)

  if (length(order_by <- attr(x, "order", exact = TRUE))) {
    #' @importFrom dbplyr translate_sql_
    order_by <- translate_sql_(order_by, con = conn, window = FALSE)
    order_by <- sub_db_identifier(order_by, conn, fields)
  }

  if (length(sorted <- attr(x, "sorted", exact = TRUE))) {
    #' @importFrom dbplyr translate_sql_
    sorted <- translate_sql_(lapply(sorted, as.name), con = conn,
                              window = FALSE)
    sorted <- sub_db_identifier(sorted, conn, fields)
  } else {
    sorted <- list()
  }

  order_by <- unique(c(order_by, sorted))

  if (length(order_by)) {
    paste(pad_left("ORDER BY"), paste(order_by, collapse = ", "))
  } else {
    NULL
  }
}
