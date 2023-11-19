as_cte <- function(x) {
  id_name <- unique_table_name("CTE")

  if (length(order_by <- get_order_by(x))) {
    is_name <- vapply(x, is.name, FALSE)
    v <- lapply(names(x)[is_name], as.name)
    names(v) <- as.character(x[is_name])
    order_by <- lapply(order_by, sub_lang, remotes = v)

    tmp <- lapply(order_by, all.names)
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

  order_by <- lapply(order_by, sub_lang, remotes = v, locals = NULL)

  ctes <- get_ctes(x)
  attr(x, "ctes") <- list()
  attr(x, "order_by") <- list()
  cte <- list(x)
  names(cte) <- id_name

  dbi_table_object(v, attr(x, "conn", exact = TRUE), data_source, fields,
                   order_by = order_by, ctes = c(ctes, cte))
}



dbi.table_is_simple <- function(x) {
  if (is.dbi.table(x)) {
    nrow(get_data_source(x)) == 1 &&
    all(vapply(x, is.name, FALSE)) &&
    all(as.character(x) %in% get_fields(x)$internal_name) &&
    #length(get_order_by(x)) == 0 &&
    length(get_where(x)) == 0
  } else {
    FALSE
  }
}



dbi.table_can_join_x <- function(x) {
  if (is.dbi.table(x)) {
    has_over <- lapply(x, attr, which = "over", exact = TRUE)
    has_over <- !vapply(has_over, is.null, FALSE)

    length(get_group_by(x)) == 0 &&
      !any(has_over)
  } else {
    FALSE
  }
}
