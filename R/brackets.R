preprocess_cols <- function(e, dbi_table, enclos, name.ok = FALSE) {
  if (is_call_to(e) == ":") {
    dbit_names <- names(dbi_table)
    if (is.name(lhs <- e[[2]])) {
      if (is.na(lhs <- chmatch(as.character(lhs), dbit_names))) {
        stop("'", e[[2]], "' - subscript out of bounds", call. = FALSE)
      }
    }
    if (is.name(rhs <- e[[3]])) {
      if (is.na(rhs <- chmatch(as.character(rhs), dbit_names))) {
        stop("'", e[[3]], "' - subscript out of bounds", call. = FALSE)
      }
    }
    return(names_list(dbi_table)[lhs:rhs])
  }

  if (is.call(e) && length(all.vars(e)) == 0L) {
    dbit_names <- names(dbi_table)
    if (is.numeric(e <- eval(e, envir = NULL, enclos = NULL))) {
      e <- dbit_names[e]
    }
    if (length(setdiff(e, dbit_names))) {
      stop("subscript out of bounds", call. = FALSE)
    }
    return(sapply(e, as.name, simplify = FALSE))
  }

  if (is.name(e)) {
    e_char <- as.character(e)

    if (substring(e_char, 1, 2) == ".." && nchar(e_char) > 2L) {
      e <- eval(as.name(substring(e_char, 3)), envir = enclos)
      return(sapply(names(dbi_table), as.name, simplify = FALSE)[e])
    }

    if (e_char %chin% names(dbi_table)) {
      if (name.ok) {
        return(sapply(e_char, as.name, simplify = FALSE))
      } else {
        stop("syntax not supported - when 'j' is a symbol and column of 'x', ",
             "data.table returns 'j' as a vector; dbi.table can only ",
             "return dbi.tables", call. = FALSE)
      }
    } else if (e_char == ".N") {
      return(list(N = as.name(".N")))
    } else {
      stop("j (the 2nd argument inside [...]) is a single symbol but there ",
           "is no column named '", e_char, "' in the dbi.table. To select ",
           "dbi.table columns using a variable in the calling scope, use ",
           "x[, ..", e_char, "] (where 'x' is your dbi.table)", call. = FALSE)
    }
  }

  if (is.call(e) || (is.list(e) && all(vapply(e, is_language, FALSE)))) {
    return(e)
  }

  stop("syntax error")

  NULL
}



handle_i_call <- function(x, i, enclos) {
  if (!is.call(i)) {
    return(x)
  }

  i <- sub_lang(i, envir = x, enclos = enclos)

  if (is_call_to(i) == "order") {
    return(handle_i_order(x, i))
  }

  handle_i_where(x, i)
}



update_order_by <- function(x, i) {
  i <- as.list(i[-1])
  i <- i[!vapply(i, is.null, FALSE)]

  if (length(i) < 1) {
    return(list())
  }

  unique(c(i, get_order_by(x)))
}



handle_i_order <- function(x, i) {
  attr(x, "order_by") <- update_order_by(x, i)
  x
}



handle_i_where <- function(x, i) {
  where <- attr(x, "where", exact = TRUE)
  where[[length(where) + 1L]] <- i
  attr(x, "where") <- where
  x
}



handle_by <- function(x, by, enclos) {
  if (is.null(by)) {
    return(list())
  }

  by <- sub_lang(by, envir = x, enclos = enclos)

  if (is.call(by)) {
    if (is_call_to(by) == "list") {
      by <- as.list(by[-1])
    } else if (is.call(by)) {
      by <- list(by)
    }
  }

  if (length(window_calls(by, dbi_connection(x)))) {
    stop("Aggregate and window functions are not allowed in 'by'")
  }

  by
}



handle_j <- function(x, j, by, enclos) {
  if (is.null(j)) {
    return(x)
  }

  j <- sub_lang(j, envir = x, enclos = enclos)
  by <- handle_by(x, by, enclos)

  a <- attributes(x)

  if (all(calls_can_aggregate(j))) {
    a$group_by <- by
  } else {
    j <- handle_over(x, j, by, a$order_by)
  }

  if (is.null(j_names <- names(j))) {
    j_names <- character(length(j))
  }

  if (any(idx <- (nchar(j_names) == 0L))) {
    j_names[idx] <- paste0("V", which(idx))
  }

  by_names <- names(by)

  j <- c(by, j)
  a$names <- c(by_names, j_names)
  attributes(j) <- a

  j
}



handle_over <- function(x, j, partition, order) {
  over <- list(partition_by = unname(partition), order_by = unname(order))

  for (k in window_calls(j, dbi_connection(x))) {
    attr(j[[k]], "over") <- over
  }

  j
}



handle_colon_equal <- function(x, i, j, by, env, x_sub) {
  if (!is.null(i)) {
    if (is_call_to(i) == "order") {
      order_by <- update_order_by(x, i)
    } else {
      stop("when using :=, if ", sQuote("i"), " is not missing ",
           "it must be a call to ", sQuote("order"))
    }
  } else {
    order_by <- NULL
  }

  by <- handle_by(x, by)

  j <- handle_over(x, as.list(j[-1]), by, order_by)

  a <- attributes(x)
  a$names <- NULL
  x <- c(x)
  x[names(j)] <- j
  a$names <- names(x)
  attributes(x) <- a

  if (is.name(x_sub)) {
    x_name <- as.character(x_sub)

    if (!is.null(env[[x_name]]) || identical(env, .GlobalEnv)) {
      assign(x_name, x, envir = env)
    } else {
      warning(sQuote(x_name), " could not be modified in place")
    }
  } else {
    warning("dbi.table could not be modified in place")
  }

  #invisible doesn't work - use data.table's workaround
  session$print <- address(x)
  x
}
