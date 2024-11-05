#' @export
merge.dbi.table <- function(x, y, by = NULL, by.x = NULL, by.y = NULL,
                            all = FALSE, all.x = all, all.y = all,
                            sort = FALSE, suffixes = c(".x", ".y"),
                            no.dups = TRUE, recursive = FALSE, ...) {
  if (!is.dbi.table(x)) {
    stop("'x' is not a 'dbi.table'")
  }

  if (missing(y)) {
    return(relational_merge(x, recursive))
  }

  if (!is.dbi.table(y)) {
    stop("'y' is not a 'dbi.table'")
  }

  names_x <- names(x)
  names_y <- names(y)

  if (anyDuplicated(names_x) || anyDuplicated(names_y)) {
    stop("the 'merge' method for 'dbi.table' requires that 'x' and 'y' ",
         "each have unique column names")
  }

  if (is.null(by) && is.null(by.x) && is.null(by.y)) {
    if (is.null(rt <- related_tables(x, y)) || nrow(rt) < 1L) {
      by.x <- by.y <- intersect(names_x, names_y)
    } else {
      by.x <- match_fields(x, rt$field_x)
      by.y <- match_fields(y, rt$field_y)
      use <- !is.na(by.x) & !is.na(by.y)
      by.x <- by.x[use]
      by.y <- by.y[use]
    }
  }

  if ((!is.null(by.x) || !is.null(by.y)) && length(by.x) != length(by.y))
    stop("'by.x' and 'by.y' are not the same length")

  if (!missing(by) && !missing(by.x))
    warning("specification of 'by' superseded by 'by.x' and 'by.y'")

  if (!is.null(by.x)) {
    if (length(by.x) == 0L || !is.character(by.x) || !is.character(by.y))
      stop("non-empty character vectors of column names are required for ",
           "'by.x' and 'by.y'")

    if (!all(by.x %chin% names_x))
      stop("Elements listed in 'by.x' must be valid column names in 'x'")

    if (!all(by.y %chin% names_y))
      stop("Elements listed in 'by.y' must be valid column names in 'y'")
  } else {
    if (!length(by)) {
      stop("a non-empty character vector of column names is required for 'by'")
    }

    if (!all(by %chin% intersect(names_x, names_y)))
      stop("Elements listed in 'by' must be valid column names in 'x' and 'y'")

    by <- unname(by)
    by.x <- by.y <- by
  }

  on <- paste(paste0("x.", by.x), paste0("y.", by.y), sep = " == ")
  on <- handy_andy(lapply(on, str2lang))

  if (!length(by.x)) {
    type <- "cross"
  } else if (all.x && all.y) {
    type <- "outer"
  } else if (all.x && !all.y) {
    type <- "left"
  } else if (!all.x && all.y) {
    type <- "right"
  } else {
    type <- "inner"
  }

  xy <- sql.join(x, y, type, on)

  x_length <- length(x)
  by_x_jdx <- chmatch(by.x, names_x)
  x_non_by_jdx <- setdiff(seq_along(x), by_x_jdx)
  by_y_jdx <- chmatch(by.y, names_y)
  y_non_by_jdx <- setdiff(seq_along(y), by_y_jdx)

  non_by <- names(xy)[c(x_non_by_jdx, x_length + y_non_by_jdx)]
  non_by <- unname(sapply(non_by, as.name, simplify = FALSE))

  if (type %chin% c("inner", "left")) {
    by <- names_list(xy)[by_x_jdx]
  } else if (type == "right") {
    by <- names_list(xy)[x_length + by_y_jdx]
  } else if (type == "outer") {
    by_x <- sapply(names(xy)[by_x_jdx], as.name, simplify = FALSE)
    by_y <- sapply(names(xy)[x_length + by_y_jdx], as.name, simplify = FALSE)
    by <- unname(mapply(call, name = "coalesce", by_x, by_y))
  } else { #for cross joins
    by <- list()
  }

  j <- c(c(by, non_by))

  # naming logical taken from merge.data.table (data.table version 1.14.10)
  start <- setdiff(names_x, by.x)
  end <- setdiff(names_y, by.y)
  dupnames <- intersect(start, end)
  if (length(dupnames)) {
    start[chmatch(dupnames, start, 0L)] <- paste0(dupnames, suffixes[1L])
    end[chmatch(dupnames, end, 0L)] <- paste0(dupnames, suffixes[2L])
  }
  dupkeyx <- intersect(by.x, end)
  if (no.dups && length(dupkeyx)) {
    end[chmatch(dupkeyx, end, 0L)] <- paste0(dupkeyx, suffixes[2L])
  }

  names(j) <- c(by.x, start, end)
  handle_j(xy, j, by = NULL, enclos = NULL)
}



merge_i_dbi_table <- function(x, i, not_i, j, by, nomatch, on, enclos) {
  names(x) <- paste0("x.", x_names <- names(x))
  names(i) <- paste0("i.", i_names <- names(i))

  if (is.null(nomatch)) {
    join_type <- "inner"
  } else if (is.na(nomatch)) {
    join_type <- "right"
  } else {
    stop("'nomatch' must be NA or NULL", call. = FALSE)
  }

  if (is.character(on)) {
    if (is.null(on_names <- names(on))) {
      on_names <- on
    }

    on <- as.list(parse(text = on))
    single_name <- vapply(on, is.name, FALSE)
    no_name <- (nchar(on_names) == 0L)

    on_names[single_name & no_name] <- as.character(on[single_name & no_name])

    on[single_name] <- mapply(call,
                              name = "==",
                              lapply(on_names[single_name], as.name),
                              on[single_name],
                              SIMPLIFY = FALSE,
                              USE.NAMES = FALSE)

  } else if ((is_call_to(on) == ".") || (is_call_to(on) == "list")) {
    on <- as.list(on[-1])
  }

  on <- lapply(on, bracket_on_validator, x_names = x_names, i_names = i_names)

  on_x <- as.character(lapply(on, `[[`, 2L))
  on_i <- as.character(lapply(on, `[[`, 3L))

  on <- lapply(on, function(u) {u[[2L]] <- as.name(paste0("x.", u[[2L]])); u})
  on <- lapply(on, function(u) {u[[3L]] <- as.name(paste0("i.", u[[3L]])); u})

  on <- handy_andy(on)

  if (not_i) {
    xi <- sql.join(x, i, type = "left", on = on, prefixes = c("x.", "i."))

    w <- lapply(paste0("i.", on_i), function(u) call("is.na", as.name(u)))
    w <- handy_andy(w)
    xi <- xi[w]

    if (is.null(j)) {
      j <- names_list(names(x), x_names)
    } else {
      j <- sub_lang(j, envir = names_list(names(x), x_names))
    }

    xi <- handle_j(xi, j, by = NULL)
  } else {
    xi <- sql.join(x, i, type = join_type, on = on)

    j_map <- names_list(xi)
    i_map <- names_list(names(i), i_names)
    j_map[names(i_map)] <- i_map
    x_map <- names_list(names(x), x_names)
    j_map[names(x_map)] <- x_map
    on_map <- names_list(paste0("i.", on_i), on_x)
    j_map[names(on_map)] <- on_map

    if (is.null(j)) {
      i_names <- setdiff(i_names, on_i)
      dups <- intersect(i_names, x_names)
      i_names[i_names %chin% dups] <- paste0("i.", i_names[i_names %chin% dups])
      j <- names_list(c(x_names, i_names))
    }

    j <- sub_lang(j, envir = j_map, specials = NULL)
    xi <- handle_j(xi, j, by = NULL)
  }

  xi
}



DT_SUPPORTED_JOIN_OPERATORS <- c("==", "<=", "<", ">=", ">")



bracket_on_validator <- function(expr, x_names, i_names) {
  if (is.name(expr)) {
    cexpr <- as.character(expr)
    if (cexpr %chin% x_names && cexpr %chin% i_names) {
      return(call("==", expr, expr))
    } else {
      stop("argument specifying columns received non-existing column: '",
           cexpr, "'")
    }
  }

  if ((op <- is_call_to(expr)) %chin% DT_SUPPORTED_JOIN_OPERATORS) {
    if (!(lhs <- as.character(expr[[2L]])) %chin% x_names) {
      stop("argument specifying columns received non-existing column: '",
           lhs, "'")
    }
    if (!(rhs <- as.character(expr[[3L]])) %chin% i_names) {
      stop("argument specifying columns received non-existing column: '",
           rhs, "'")
    }
    return(expr)
  } else {
    stop("invalid join operator [", op, "]; the allowed operators are ",
         "[", paste(DT_SUPPORTED_JOIN_OPERATORS, collapse = ", "), "]")
  }

  NULL
}
