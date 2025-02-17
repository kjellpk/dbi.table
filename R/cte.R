as_cte <- function(x) {
  id_name <- unique_table_name("CTE")

  if (length(order_by <- get_order_by(x))) {
    is_name <- vapply(x, is.name, FALSE)
    v <- lapply(names(x)[is_name], as.name)
    names(v) <- as.character(x[is_name])
    order_by <- lapply(order_by, sub_lang, envir = v, specials = NULL)

    tmp <- lapply(order_by, all.names)
    keep <- vapply(tmp, function(u, v) all(u %in% v), FALSE, v = names(x))
    order_by <- order_by[keep]

    if (any(!keep)) {
      warning("some order information lost during conversion to cte")
    }
  }

  data_source <- data.frame(clause = "FROM",
                            id = I(list(DBI::Id(id_name))),
                            id_name = id_name,
                            on = I(list(NULL)))

  field_names <- names(x)
  fields <- data.frame(internal_name = paste0(session$key_base,
                                              seq_along(field_names)),
                       id_name = id_name,
                       field = field_names)

  v <- lapply(fields$internal_name, as.name)
  names(v) <- fields$field

  order_by <- lapply(order_by, sub_lang, envir = v, specials = NULL)

  ctes <- get_ctes(x)
  attr(x, "ctes") <- list()
  attr(x, "order_by") <- list()
  cte <- list(x)
  names(cte) <- id_name

  dbi_table_object(cdefs = v, conn = get_connection(x),
                   data_source = data_source, fields = fields,
                   order_by = order_by, ctes = c(ctes, cte))
}



dbi_table_is_simple <- function(x) {
  if (!is.dbi.table(x)) {
    return(FALSE)
  }

  has_over <- vapply(c(x),
                     function(u) !is.null(attr(u, "over", exact = TRUE)),
                     FALSE)
  group_by <- get_group_by(x)
  distinct <- attr(x, "distinct", exact = TRUE)

  (length(group_by) == 0L) && !any(has_over) && !distinct
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
