preprocess_j <- function(e, dbi_table, enclos, single.ok = FALSE) {
  if (is_call_to(e) == ":=") {
    return(e)
  }

  e <- preprocess_common(e, dbi_table, enclos, single.ok)

  if (is.null(e_names <- names(e))) {
    e_names <- character(length(e))
  }

  if (any(idx <- !nzchar(e_names))) {
    nm <- vapply(e, function(u) if (is.name(u)) as.character(u) else "", "")
    ndx <- !nzchar(nm)
    nm[ndx] <- paste0("V", which(ndx))
    e_names[idx] <- nm[idx]
  }

  if (anyDuplicated(e_names)) {
    e_names <- make.unique(e_names)
  }

  names(e) <- e_names
  e
}



preprocess_by <- function(e, dbi_table, enclos, single.ok = FALSE) {
  e <- preprocess_common(e, dbi_table, enclos, single.ok)

  if (is.null(e_names <- names(e))) {
    e_names <- character(length(e))
  }

  if (any(idx <- !nzchar(e_names))) {
    av <- lapply(e, all.vars, functions = TRUE)
    av <- mapply(grep, x = av,
                 MoreArgs = list(pattern = "^eval$|^[^[:alpha:]. ]",
                                 invert = TRUE, value = TRUE),
                 SIMPLIFY = FALSE, USE.NAMES = FALSE)

    if (anyNA(av <- vapply(av, `[`, "", 1L))) {
      nadx <- is.na(av)
      av[nadx] <- vapply(e[nadx], deparse1, "")
    }
    e_names[idx] <- av[idx]
  }

  if (anyDuplicated(e_names)) {
    e_names <- make.unique(e_names)
  }

  names(e) <- e_names
  e
}



preprocess_common <- function(e, dbi_table, enclos, single.ok) {
  if (is_call_to(e) %chin% c(".", "list")) {
    e <- as.list(e)[-1]
  }

  if (is.list(e) && all(vapply(e, is_language, FALSE))) {
    return(e)
  }

  if (is_call_to(e) == ":=") {
    return(e)
  }

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

  if (is.call(e) && single.ok) {
    return(list(e))
  }

  if (is.name(e)) {
    e_char <- as.character(e)

    if (substring(e_char, 1, 2) == ".." && nchar(e_char) > 2L) {
      e <- eval(as.name(substring(e_char, 3)), envir = enclos)
      return(sapply(names(dbi_table), as.name, simplify = FALSE)[e])
    }

    if (e_char %chin% names(dbi_table)) {
      if (single.ok) {
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

  if (is.numeric(e) && all(e %in% seq_along(dbi_table))) {
    return(names_list(dbi_table)[e])
  }

  stop("syntax error", call. = FALSE)

  NULL
}



handle_i_call <- function(x, i, enclos) {
  if (!is.call(i)) {
    return(x)
  }

  if (is_call_to(i) == "order") {
    return(handle_i_order(x, i, enclos))
  }

  handle_i_where(x, i)
}



update_order_by <- function(x, i, enclos) {
  i <- as.list(i[-1])
  i <- i[!vapply(i, is.null, FALSE)]

  if (length(i) < 1) {
    return(list())
  }

  unique(c(i, get_order_by(x)))
}



handle_i_order <- function(x, i, enclos) {
  attr(x, "order_by") <- update_order_by(x, i, enclos)
  x
}



handle_i_where <- function(x, i) {
  where <- get_where(x)
  where[[length(where) + 1L]] <- i
  attr(x, "where") <- where
  x
}



handle_by <- function(x, by, enclos) {
  if (is.null(by)) {
    return(list())
  }

  by <- sub_lang(by, envir = x, enclos = enclos)

  if (length(window_calls(by, dbi_connection(x)))) {
    stop("Aggregate and window functions are not allowed in 'by'",
         call. = FALSE)
  }

  by
}



handle_j <- function(x, j, by, enclos) {
  if (is.null(j)) {
    return(x)
  }

  if (is.null(j_names <- names(j))) {
    j_names <- paste0("V", seq_along(j))
  }

  if (any(idx <- (!nzchar(j_names) | is.na(j_names)))) {
    j_names[idx] <- paste0("V", which(idx))
  }

  j <- sub_lang(j, envir = x, enclos = enclos)
  by <- handle_by(x, by, enclos)

  a <- attributes(x)

  if (all(calls_can_aggregate(j))) {
    a$group_by <- by
  } else {
    j <- handle_over(x, j, by, a$order_by)
  }

  j <- c(by, j)
  a$names <- c(names(by), j_names)
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
      order_by <- update_order_by(x, i, enclos = env)
    } else {
      stop("when using :=, if 'i' is not missing it must be a call to 'order'",
           call. = FALSE)
    }
  } else {
    order_by <- NULL
  }

  by <- handle_by(x, by)

  if (length(j) == 3L) {
    lhs <- j[[2]]
    if (is.name(lhs)) {
      lhs <- as.character(lhs)
    } else if (!length(all.vars(lhs))) {
      lhs <- as.character(eval(lhs, envir = env))
    } else if (is_call_to(lhs) %chin% c(".", "list")) {
      lhs <- vapply(as.list(lhs)[-1], deparse1, "")
    } else {
      stop("the left-hand-side of ':=' should be a character vector ",
           "or a list of names", call. = FALSE)
    }

    if (is_call_to(j[[3L]]) %chin% c(".", "list")) {
      j <- as.list(j[[3L]])[-1]
    } else {
      j <- list(j[[3L]])
    }

    names(j) <- lhs
  } else {
    j <- as.list(j)[-1]
  }

  if (anyDuplicated(names(j))) {
    stop("duplicated assignments in ':='", call. = FALSE)
  }

  j_null <- j[jdx <- vapply(j, is.null, FALSE)]
  j <- j[!jdx]

  if (!all(vapply(j, is_language, FALSE))) {
    stop("the right-hand-side of ':=' must be an expression or a ",
         "list of expressions", call. = FALSE)
  }

  j <- sub_lang(j, x, enclos = env)
  j <- handle_over(x, j, by, order_by)

  a <- attributes(x)
  a$names <- NULL
  x <- c(x)
  x[names(j)] <- j
  x <- x[setdiff(names(x), names(j_null))]
  a$names <- names(x)
  attributes(x) <- a

  if (is.name(x_sub)) {
    x_name <- as.character(x_sub)

    if (!is.null(env[[x_name]]) || identical(env, .GlobalEnv)) {
      if (is_dbi_catalog(env[["..catalog"]])) {
        stop("'dbi.table's in 'dbi.catalog's cannot be modified by reference",
             call. = FALSE)
      } else {
        assign(x_name, x, envir = env)
      }
    } else {
      warning("'", x_name, "' could not be modified in place")
    }
  } else {
    warning("dbi.table could not be modified in place")
  }

  #invisible doesn't work - use data.table's workaround
  session$print <- address(x)
  x
}
