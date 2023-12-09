preprocess_subset_arg <- function(arg, arg_sub, x, env) {
  if (missing(arg)) {
    NULL
  } else if (inherits(try(arg, silent = TRUE), "try-error")) {
    sub_lang(arg_sub, remotes = x, locals = env)
  } else {
    arg
  }
}



handle_subset <- function(x, i, j, by) {
  if (is.null(j) && !is.null(by)) {
    stop("cannot handle ", sQuote("by"), " when ", sQuote("j"),
         " is missing or ", sQuote("NULL"))
  }

  x <- handle_i(x, i)
  x <- handle_j(x, j, by)

  x
}



handle_i <- function(x, i) {
  if (is.null(i))
    return(x)

  stopifnot(is.language(i))

  switch(as.character(i[[1]]),
    order = handle_i_order(x, as.list(i[-1])),
    list = handle_i_list(x, as.list(i[-1])),
    handle_i_list(x, list(i))
  )
}



handle_i_list <- function(x, i) {
  attr(x, "where") <- i
  x
}



handle_i_order <- function(x, i) {
  i <- i[!vapply(i, is.null, FALSE)]
  stopifnot(all(vapply(i, is.language, FALSE)))

  if (length(i) < 1) {
    attr(x, "order_by") <- list()
    return(x)
  }

  attr(x, "order_by") <- unique(c(i, get_order_by(x)))
  x
}



handle_by <- function(x, by) {
  if (is.null(by)) {
    return(NULL)
  }

  by <- handle_cols(x, by, "by")

  if (any(window_calls(by, get_connection(x)))) {
    stop("Aggregate and window functions are not allowed in ", sQuote("by"))
  }

  by
}



handle_vector <- function(x, v) {
  if (any(vapply(v <- c(x)[v], is.null, FALSE))) {
    stop("subscript out of bounds")
  }

  v
}



handle_cols <- function(x, cols, arg_name = "<unknown>") {
  if (is.call(cols)) {
    if (as.character(cols[[1]]) == "list") {
      cols <- as.list(cols[-1])
    } else {
      cols <- list(cols)
    }
  } else if (is.vector(cols)) {
    cols <- handle_vector(x, cols)
  } else {
    stop(sQuote(arg_name), " must be a vector or a call")
  }

  cols
}



handle_j <- function(x, j, by) {
  if (is.null(j))
    return(x)

  j <- handle_cols(x, j, "j")
  by <- handle_by(x, by)
  a <- attributes(x)

  if (all(calls_can_aggregate(j))) {
    a$group_by <- by
  } else {
    over <- list(partition_by = by,
                 order_by = a$order_by)

    for (k in which(window_calls(j, get_connection(x)))) {
      attr(j[[k]], "over") <- over
    }
  }

  j <- c(by, j)
  n <- names(j)
  if (any(idx <- (nchar(n) == 0L))) {
    n[idx] <- paste0("V", which(idx))
  }
  a$names <- n
  attributes(j) <- a

  j
}



handle_colon_equal <- function(x, i, j, by) {
  j <- as.list(j[-1])
  a <- attributes(x)
  a$names <- NULL
  x <- c(x)
  x[names(j)] <- j
  a$names <- names(x)
  attributes(x) <- a

  x
}