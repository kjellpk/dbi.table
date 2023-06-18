as_cte <- function(x) {
  id_name <- unique_table_name("CTE")

  if (length(order_by <- attr(x, "order", exact = TRUE))) {
    is_name <- vapply(x, is.name, FALSE)
    v <- lapply(names(x)[is_name], as.name)
    names(v) <- as.character(x[is_name])
    order_by <- lapply(order_by, sub_lang, cols = v)

    tmp <- lapply(order_by, get_names)
    keep <- vapply(tmp, function(u, v) all(u %in% v), FALSE, v = names(x))
    order_by <- order_by[keep]

    if (any(!keep)) {
      warning("some order information lost during conversion to cte")
    }
  }

  #' @importFrom DBI Id
  data_source <- data.frame(clause = "FROM",
                            id = I(list(Id(table = id_name))),
                            id_name = id_name,
                            on = I(list(NULL)))

  field_names <- names(x)
  fields <- data.frame(internal_name = paste0("FN", seq_along(field_names)),
                       id_name = id_name,
                       field = field_names)

  v <- lapply(fields$internal_name, as.name)
  names(v) <- fields$field

  order_by <- prepare_calls(order_by, v, emptyenv())

  ctes <- attr(x, "ctes")
  attr(x, "ctes") <- list()
  attr(x, "order") <- list()
  cte <- list(x)
  names(cte) <- id_name

  dbi_table_object(v, get_connection(x), data_source, fields,
                   order_by = order_by, ctes = c(ctes, cte))
}



dbi.table_is_simple <- function(x) {
  if (is.dbi.table(x)) {
    nrow(attr(x, "data_source", exact = TRUE)) == 1 &&
    all(vapply(x, is.name, FALSE)) &&
    all(as.character(x) %in% attr(x, "fields", exact = TRUE)$internal_name) &&
    length(attr(x, "by", exact = TRUE)) == 0 &&
    length(attr(x, "where", exact = TRUE)) == 0
  } else {
    FALSE
  }
}



dbi.table_can_join_x <- function(x) {
  if (is.dbi.table(x)) {
    has_over <- lapply(x, attr, which = "over", exact = TRUE)
    has_over <- !vapply(has_over, is.null, FALSE)

    length(attr(x, "by", exact = TRUE)) == 0 &&
    !any(has_over)
  } else {
    FALSE
  }
}
