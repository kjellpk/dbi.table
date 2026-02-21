preprocess_j <- function(e, dbi_table, enclos, single.ok = FALSE) {
  if (e %is_call_to% ":=") {
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
  if (e %is_call_to% c(".", "list")) {
    e <- as.list(e)[-1]
  }

  if (is.list(e) && all(vapply(e, is_language, FALSE))) {
    return(e)
  }

  if (e %is_call_to% ":=") {
    return(e)
  }

  if (e %is_call_to% ":") {
    dbit_names <- names(dbi_table)
    if (is.name(lhs <- e[[2]])) {
      if (is.na(lhs <- match(as.character(lhs), dbit_names))) {
        stop("'", e[[2]], "' - subscript out of bounds", call. = FALSE)
      }
    }
    if (is.name(rhs <- e[[3]])) {
      if (is.na(rhs <- match(as.character(rhs), dbit_names))) {
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

    if (e_char %in% names(dbi_table)) {
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

  if (i %is_call_to% "order") {
    return(handle_i_order(x, i, enclos))
  }

  handle_i_where(x, i)
}



update_order_by <- function(x, i, include_key = FALSE) {
  order_by <- NULL

  if (include_key && length(x_key <- get_key(x))) {
    x_key <- c(x)[x_key]
  } else {
    x_key <- NULL
  }

  if (is.call(i)) {
    i <- as.list(i[-1])

    if (length(i) == 1L && is.null(x[[1L]])) {
      return(x_key)
    }

    order_by <- c(order_by, i[!vapply(i, is.null, FALSE)])
  }

  unique(c(order_by, get_order_by(x), x_key))
}



handle_i_order <- function(x, i, enclos) {
  order_by <- update_order_by(x, i)

  if (length(x_key <- get_key(x))) {
    x_key <- names_list(x_key)
    m <- min(length(x_key), length(order_by))

    if (!all(mapply(identical, sub_lang(x_key[m], c(x)), order_by[m]))) {
      attr(x, "sorted") <- NULL
    }
  }

  attr(x, "order_by") <- order_by
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

  by <- sub_lang(by, x, get_specials(x), enclos)

  if (length(window_calls(by, x))) {
    stop("Aggregate and window functions are not allowed in 'by'",
         call. = FALSE)
  }

  by
}



handle_j <- function(x, j, by, enclos) {
  if (is.null(j)) {
    return(x)
  }

  x_key_cols <- c(x)[get_key(x)]

  if (is.null(j_names <- names(j))) {
    j_names <- paste0("V", seq_along(j))
  }

  if (any(idx <- (!nzchar(j_names) | is.na(j_names)))) {
    j_names[idx] <- paste0("V", which(idx))
  }

  j <- sub_lang(j, x, get_specials(x), enclos)
  by <- handle_by(x, by, enclos)

  a <- attributes(x)

  if (all(calls_can_aggregate(j))) {
    a$group_by <- by
  } else {
    j <- handle_over(x, j, by, update_order_by(x, NULL, include_key = TRUE))
  }

  names(j) <- j_names
  j <- c(by, j)

  j_key <- match(x_key_cols, j, nomatch = 0L)
  j_key <- j_key[j_key > 0L]
  j_key <- names(j)[j_key]

  dbi_table_object(cdefs = j, conn = a$conn, data_source = a$data_source,
                   fields = a$fields, key = j_key, distinct = a$distinct,
                   where = a$where, group_by = a$group_by,
                   order_by = a$order_by, ctes = a$ctes)
}



handle_over <- function(x, j, partition, order) {
  over <- list(partition_by = unname(partition), order_by = unname(order))

  for (k in window_calls(j, x)) {
    attr(j[[k]], "over") <- over
  }

  j
}



handle_the_walrus <- function(x, i, j, by, env, x_sub) {

  if (length(i) && !(i %is_call_to% "order")) {
    stop("when using :=, if 'i' is not missing it must be a call to 'order'",
         call. = FALSE)
  }

  order_by <- update_order_by(x, i, include_key = TRUE)
  by <- handle_by(x, by)

  if (length(j) == 3L) {
    lhs <- j[[2]]
    if (is.name(lhs)) {
      lhs <- as.character(lhs)
    } else if (!length(all.vars(lhs))) {
      lhs <- as.character(eval(lhs, envir = env))
    } else if (lhs %is_call_to% c(".", "list")) {
      lhs <- vapply(as.list(lhs)[-1], deparse1, "")
    } else {
      stop("the left-hand-side of ':=' should be a character vector ",
           "or a list of names", call. = FALSE)
    }

    if (j[[3L]] %is_call_to% c(".", "list")) {
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

  j <- sub_lang(j, x, get_specials(x), env)
  j <- handle_over(x, j, by, order_by)

  a <- attributes(x)
  a$names <- NULL
  x <- c(x)
  x[names(j)] <- j
  x <- x[setdiff(names(x), names(j_null))]
  a$names <- names(x)
  attributes(x) <- a

  if (is.symbol(x_sub)) {
    x_name <- as.character(x_sub)
    x_env <- find_environment(x_name, class = "dbi.table", envir = env)

    if (!is.null(x_env)) {
      if (is_dbi_schema(x_env)) {
        search_path_envs <- lapply(search(), as.environment)

        if (any(vapply(search_path_envs, identical, FALSE, y = x_env))) {
          x_env <- env
        }
      }

      res <- stry(assign(x_name, x, pos = x_env))
      if (inherits(res, "try-error")) {
        warning(attr(res, "condition")$message, call. = FALSE)
      }
    }
  }

  #invisible doesn't work - use data.table's workaround
  session$print <- x
  x
}
