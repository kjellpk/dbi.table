handle_subset <- function(x, x_sub, i, j, by, env) {
  if (is.null(j) && !is.null(by)) {
    stop("cannot handle ", sQuote("by"), " when ", sQuote("j"),
         " is missing or ", sQuote("NULL"))
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
      attr(x, "group_by") <- by
      attr(x, "order_by") <- intersect(get_order_by(x), unname(by))
    } else {
      over <- list(partition_by = unname(by),
                   order_by = unname(get_order_by(x)))

      for (k in which(window_calls(j, get_connection(x)))) {
        attr(j[[k]], "over") <- over
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
    #cols <- lapply(cols, sub_lang, remotes = x, locals = env)
    names(cols) <- col_names
  }

  cols
}



handle_i <- function(x, i, env) {
  stopifnot(is.call(i))

  switch(as.character(i[[1]]),
    order = handle_i_order(x, as.list(i[-1]), env),
    list = handle_i_list(x, as.list(i[-1]), env),
    handle_i_list(x, list(i), env)
  )
}



handle_i_list <- function(x, i, env) {
  attr(x, "where") <- i
  x
}



handle_i_order <- function(x, i, env) {
  i <- i[!vapply(i, is.null, FALSE)]
  stopifnot(all(vapply(i, is.language, FALSE)))

  if (length(i) < 1) {
    attr(x, "order_by") <- list()
    return(x)
  }

  i <- c(i, get_order_by(x))
  attr(x, "order_by") <- i[!duplicated(i)]

  x
}
