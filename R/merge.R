#' @name merge
#'
#' @aliases merge.dbi.table
#'
#' @title Merge two dbi.tables
#'
#' @description
#'   Merge two \code{\link{dbi.table}}s. By default, the columns to merge on are
#'   determined by the first of the following cases to apply.
#'
#'   \enumerate{
#'     \item If \code{x} and \code{y} are each unmodified \code{dbi.table}s in
#'           the same \code{dbi.catalog} and if there is a single foreign key
#'           relating \code{x} and \code{y} (either \code{x} referencing
#'           \code{y}, or \code{y} referencing \code{x}), then it is used to set
#'           \code{by.x} and \code{by.y}.
#'
#'     \item If \code{x} and \code{y} have shared key columns, then they are
#'           used to set \code{by} (that is,
#'           \code{by = intersect(key(x), key(y))} when
#'           \code{intersect(key(x), key(y))} has length greater than zero).
#'
#'     \item If \code{x} has a key, then it is used to set \code{by} (that is,
#'           \code{by = key(x)} when \code{key(x)} has length greater than
#'           zero).
#'
#'     \item If \code{x} and \code{y} have columns in common, then they are used
#'           to set
#'           \code{by} (that is, \code{by = intersect(names(x), names(y))} when
#'           \code{intersect(names(x), names(y))} has length greater than zero).
#'   }
#'
#'   Use the \code{by}, \code{by.x}, and \code{by.y} arguments explicitly to
#'   override this default.
#'
#' @param x,y
#'   \code{\link{dbi.table}}s sharing the same DBI connection. If \code{y} is
#'   not a \code{dbi.table} but does inherit from \code{data.frame}, then it is
#'   coerced to a \code{dbi.table} using \code{\link{as.dbi.table}}. If \code{y}
#'   is missing, a merge is performed for each of \code{x}'s foreign keys.
#'
#' @param by
#'   a character vector of shared column names in \code{x} and \code{y} to merge
#'   on.
#'
#' @param by.x,by.y
#'   character vectors of column names in \code{x} and \code{y} to merge on.
#'
#' @param all
#'   a logical value. \code{all = TRUE} is shorthand to save setting both
#'   \code{all.x = TRUE} and \code{all.y = TRUE}.
#'
#' @param all.x
#'   a logical value. When \code{TRUE}, rows from \code{x} that do not have a
#'   matching row in \code{y} are included. These rows will have \code{NA}s in
#'   the columns that are filled with values from \code{y}. The default is
#'   \code{FALSE} so that only rows with data from both \code{x} and \code{y}
#'   are included in the output.
#'
#' @param all.y
#'   a logical value. Analogous to \code{all.x} above.
#'
#' @param sort
#'   a logical value. When TRUE (default), the key of the merged
#'   \code{dbi.table} is set to the \code{by} / \code{by.x} columns.
#'
#' @param suffixes
#'   a length-2 character vector. The suffixes to be used for making
#'   non-\code{by} column names unique. The suffix behavior works in a similar
#'   fashion to the \code{\link[base]{merge.data.frame}} method.
#'
#' @param no.dups
#'   a logical value. When \code{TRUE}, suffixes are also appended to
#'   non-\code{by.y} column names in \code{y} when they have the same column
#'   name as any \code{by.x}.
#'
#' @param recursive
#'   a logical value. Only used when \code{y} is missing. When \code{TRUE},
#'   \code{merge} is called on each \code{dbi.table} prior to merging with
#'   \code{x}. See examples.
#'
#' @param \dots
#'   additional arguments are passed to \code{\link{as.dbi.table}}.
#'
#' @return
#'   a \code{\link{dbi.table}}.
#'
#' @details
#'   \code{merge.dbi.table} uses \code{\link{sql.join}} to join \code{x} and
#'   \code{y} then formats the result set to match the typical \code{merge}
#'   output.
#'
#' @seealso
#'   \code{\link[data.table]{merge.data.table}},
#'   \code{\link[base]{merge.data.frame}}
#'
#' @examples
#'   chinook <- dbi.catalog(chinook.duckdb)
#'
#'   #The Album table has a foreign key constriant that references Artist
#'   merge(chinook$main$Album, chinook$main$Artist)
#'
#'   #When y is omitted, x's foreign key relationship is used to determine y
#'   merge(chinook$main$Album)
#'
#'   #Track has 3 foreign keys: merge with Album, Genre, and MediaType
#'   merge(chinook$main$Track)
#'
#'   #Track references Album but not Artist, Album references Artist
#'   #This dbi.table includes the artist name
#'   merge(chinook$main$Track, recursive = TRUE)
#'
#' @export
merge.dbi.table <- function(x, y, by = NULL, by.x = NULL, by.y = NULL,
                            all = FALSE, all.x = all, all.y = all,
                            sort = TRUE, suffixes = c(".x", ".y"),
                            no.dups = TRUE, recursive = FALSE, ...) {
  if (missing(y) || is.null(y)) {
    return(relational_merge(x, recursive))
  }

  if (!is.dbi.table(y)) {
    y <- as.dbi.table(y, ...)
  }

  dots <- list(...)

  if (!is.null(dots$allow.cartesian)) {
    warning("the value of 'allow.cartesian' was ignored")
  }

  if (!is.null(dots$incomparables)) {
    warning("non-NULL value of 'incomparables' was ignored")
  }

  x_names <- names(x)
  y_names <- names(y)

  if (anyDuplicated(x_names) || anyDuplicated(y_names)) {
    stop("the 'merge' method for 'dbi.table' requires that 'x' and 'y' ",
         "each have unique column names")
  }

  if (is.null(by) && is.null(by.x) && is.null(by.y)) {

    if (is_pristine(x) && is_pristine(y)) {
      if (length(rt <- related_tables(x, y))) {
        if (nrow(rt) == 1L) {
          by.x <- rt[[1L, "x_columns"]]
          by.y <- rt[[1L, "y_columns"]]
        }
      }
    }

    if (is.null(by.x) && is.null(by.y)) {
      by <- intersect(x_key <- get_key(x), get_key(y))

      if (!length(by)) {
        by <- x_key
      }

      if (!length(by)) {
        by <- intersect(x_names, y_names)
      }

      if (length(by) < 1L || !is.character(by)) {
        stop("a non-empty vector of column names for 'by' is required")
      }

      if (!all(by %in% x_names)) {
        stop("at least one column listed in 'by' is not present in 'x'")
      }
      if (!all(by %in% y_names)) {
        stop("at least one column listed in 'by' is not present in 'y'")
      }

      by <- unname(by)
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

    if (!all(by.x %in% x_names))
      stop("Elements listed in 'by.x' must be valid column names in 'x'")

    if (!all(by.y %in% y_names))
      stop("Elements listed in 'by.y' must be valid column names in 'y'")
  } else {
    if (!length(by)) {
      stop("a non-empty character vector of column names is required for 'by'")
    }

    if (!all(by %in% intersect(x_names, y_names)))
      stop("Elements listed in 'by' must be valid column names in 'x' and 'y'")

    by <- unname(by)
    by.x <- by.y <- by
  }

  on <- paste(paste0("`x.", by.x, "`"), paste0("`y.", by.y, "`"), sep = " == ")
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

  xy <- sql_join(x, y, type, on, c("x.", "y."), NULL)

  xy_x <- names_list(names(xy)[seq_along(x)], x_names)
  xy_y <- names_list(names(xy)[-seq_along(x)], y_names)

  start <- setdiff(x_names, by.x)
  end <- setdiff(y_names, by.y)

  if (type %in% c("inner", "left")) {
    by <- xy_x[by.x]
  } else if (type == "right") {
    by <- xy_y[by.y]
    names(by) <- by.x
  } else if (type == "outer") {
    by <- mapply(function(u, v) {
      if (!is.null(u) && !is.null(v))
        return(call("coalesce", u, v))
      if (!is.null(u))
        return(u)
      NULL
    }, u = xy_x[by.x], v = xy_y[by.y], SIMPLIFY = FALSE)
  } else { #for cross joins
    by <- list()
  }

  xy <- handle_j(xy, c(by, xy_x[start], xy_y[end]), NULL, NULL)

  # naming logical taken from merge.data.table (data.table version 1.14.10)
  by_names <- names(by)
  dupnames <- intersect(start, end)

  if (length(dupnames)) {
    start[match(dupnames, start, 0L)] <- paste0(dupnames, suffixes[1L])
    end[match(dupnames, end, 0L)] <- paste0(dupnames, suffixes[2L])
  }
  dupkeyx <- intersect(by.x, end)
  if (no.dups && length(dupkeyx)) {
    end[match(dupkeyx, end, 0L)] <- paste0(dupkeyx, suffixes[2L])
  }

  names(xy) <- c(by_names, start, end)

  if (isTRUE(sort)) {
    attr(xy, "sorted") <- by_names
  }

  xy
}



merge_i_dbi_table <- function(x, i, not_i, j, by, nomatch, on, enclos) {
  x_key <- get_key(x)

  if (is.null(on)) {
    if (length(x_key)) {
      if (is.null(i_key <- get_key(i))) {
        i_key <- names(i)
      }

      idx <- seq_len(min(length(x_key), length(i_key)))
      on <- paste(x_key[idx], i_key[idx], sep = " == ")
    } else {
      stop("when 'on' is NULL, 'x' must be keyed", call. = FALSE)
    }
  }

  names(x) <- paste0("x.", x_names <- names(x))
  names(i) <- paste0("i.", i_names <- names(i))

  if (is.null(nomatch)) {
    join_type <- "inner"
  } else if (is.na(nomatch)) {
    join_type <- "right"
  } else {
    stop("'nomatch' must be NA or NULL", call. = FALSE)
  }

  if (on %is_call_to% c(".", "list")) {
    on <- as.list(on[-1L])
  }

  if (is.null(on_names <- names(on))) {
    on_names <- character(length(on))
  }

  if (is.character(on)) {
    on <- as.list(str2expression(on))
  }

  symbols <- vapply(on, is.symbol, FALSE)
  needs_name <- (nchar(on_names) == 0L & symbols)
  on_names[needs_name] <- as.character(on[needs_name])

  on[symbols] <- mapply(call,
                        name = "==",
                        lapply(on_names[symbols], as.symbol),
                        on[symbols],
                        SIMPLIFY = FALSE,
                        USE.NAMES = FALSE)

  on <- lapply(on, extract_on_validator, x_names = x_names, i_names = i_names)

  on_x <- as.character(lapply(on, `[[`, 2L))
  on_i <- as.character(lapply(on, `[[`, 3L))

  on <- lapply(on, function(u) {
    u[[2L]] <- as.name(paste0("x.", u[[2L]]))
    u
  })

  on <- lapply(on, function(u) {
    u[[3L]] <- as.name(paste0("i.", u[[3L]]))
    u
  })

  on <- handy_andy(on)

  if (not_i) {
    xi <- sql.join(x, i, type = "left", on = on, prefixes = c("x.", "i."))

    w <- lapply(paste0("i.", on_i), function(u) call("is.na", as.name(u)))
    w <- handy_andy(w)
    xi <- xi[w]

    if (is.null(j)) {
      j <- names_list(names(x), x_names)
    } else {
      j <- sub_lang(j, names_list(names(x), x_names), get_specials(x))
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
      i_names[i_names %in% dups] <- paste0("i.", i_names[i_names %in% dups])
      j <- names_list(c(x_names, i_names))
    }

    j <- sub_lang(j, j_map)
    xi <- handle_j(xi, j, by = NULL)
  }

  attr(xi, "sorted") <- x_key
  xi
}



DT_SUPPORTED_JOIN_OPERATORS <- c("==", "<=", "<", ">=", ">")



extract_on_validator <- function(expr, x_names, i_names) {
  if (is.name(expr)) {
    cexpr <- as.character(expr)
    if (cexpr %in% x_names && cexpr %in% i_names) {
      return(call("==", expr, expr))
    } else {
      stop("argument specifying columns received non-existing column: '",
           cexpr, "'")
    }
  }

  if (expr %is_call_to% DT_SUPPORTED_JOIN_OPERATORS) {
    op <- as.character(expr[[1L]])

    if (!(lhs <- format(expr[[2L]])) %in% x_names) {
      stop("argument specifying columns received non-existing column: '",
           lhs, "'")
    }
    if (!(rhs <- format(expr[[3L]])) %in% i_names) {
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



is_pristine <- function(x) {
  f <- get_fields(x)
  nn <- names_list(f$internal_name, f$field)
  nrow(get_data_source(x)) == 1L && setequal(c(x), nn)
}
