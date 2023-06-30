bracket_subset <- function(x, i = NULL, j = NULL, by = NULL, env) {

  if (is.null(j) && !is.null(by)) {
    stop("cannot handle ", sQuote("by"), " when ", sQuote("j"),
        " is missing or ", sQuote("NULL"))
  }

  if (!dbi.table_is_simple(x)) {
    x <- as_cte(x)
  }

  if (!is.null(i)) {
    x <- handle_i(x, i, env)
  }

  if (!is.null(by)) {
    by <- handle_cols(x, by, env)

    if (any(window_calls(by, get_connection(x)))) {
      stop("Aggregate and window functions are not allowed in ", sQuote("by"))
    }
  }

  if (!is.null(j)) {
    j <- handle_cols(x, j, env)

    if (all(calls_can_aggregate(j))) {
      attr(x, "order_by") <- by
    } else {
      over <- list(partition_by = unname(by),
                   order_by = unname(get_order_by(x)))

      for (i in which(window_calls(j, get_connection(x)))) {
        attr(j[[i]], "over") <- over
      }
    }

    j <- c(by, j)

    idx <- (nchar(nj <- names(j)) == 0L)
    nj[idx] <- paste0("V", seq_len(sum(idx)))

    a <- attributes(x)
    a$names <- nj
    attributes(j) <- a
    x <- j
  }

  x
}



handle_cols <- function(x, cols, env) {

  if (is.numeric(cols)) {
    if (all(cols %in% seq_len(ncol(x)))) {
      cols <- c(x)[cols]
    } else {
      stop("out of range")
    }

  } else if (is.character(cols)) {
    if (all(cols %in% names(x))) {
      cols <- c(x)[cols]
    } else {
      stop("out of range")
    }

  } else if (is.language(cols)) {
    if (is.call(cols) && as.character(cols[[1]]) %in% c(".", "list")) {
      cols <- as.list(cols)[-1]
    } else {
      cols <- list(cols)
    }

    if (is.null(col_names <- names(cols))) {
      col_names <- character(length(cols))
    }

    idx <- (nchar(col_names) == 0L) & vapply(cols, is.name, FALSE)
    col_names[idx] <- as.character(cols[idx])

    cols <- prepare_calls(cols, x, env)
    names(cols) <- col_names
  }

  cols
}



handle_i <- function(x, i_sub, env) {
  top_level_fun <- as.character(i_sub[[1]])

  if (top_level_fun == "order") {
    handle_i_order(x, as.list(i_sub[-1]), env)
  } else if (top_level_fun %in% c(".", "list")) {
    handle_i_list(x, as.list(i_sub[-1]), env)
  } else {
    handle_i_list(x, list(i_sub), env)
  }
}



handle_i_list <- function(x, i_list, env) {
  attr(x, "where") <- prepare_calls(i_list, x, env)
  x
}



handle_i_order <- function(x, i_list, env) {
  i_list <- i_list[!vapply(i_list, is.null, FALSE)]
  stopifnot(all(vapply(i_list, is.language, FALSE)))

  if (length(i_list) < 1) {
    attr(x, "order_by") <- list()
    return(x)
  }

  i_list <- c(prepare_calls(i_list, x, env), get_order_by(x))
  attr(x, "order_by") <- i_list[!duplicated(i_list)]
  x
}
