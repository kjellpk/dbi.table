triage_brackets <- function(x, i, j, by, env = NULL, x_sub = NULL) {
  if (is.call(j) && j[[1]] == ":=") {
    return(handle_colon_equal(x, i, j, by, env, x_sub))
  }

  if (is.null(j) && !is.null(by)) {
    stop("cannot handle ", sQuote("by"), " when ", sQuote("j"),
         " is missing or ", sQuote("NULL"))
  }

  x <- handle_i(x, i)
  handle_j(x, j, by)
}



handle_i <- function(x, i) {
  if (is.null(i)) {
    return(x)
  }

  stopifnot(is.call(i))

  if (as.character(i[[1]]) == "order") {
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



handle_by <- function(x, by) {
  if (is.null(by)) {
    return(list())
  }

  if (is_call_to(by) == "list") {
    by <- as.list(by[-1])
  } else if (is.call(by)) {
    by <- list(by)
  } else if (is.name(by)) {
    by_name <- names(x)[c(x) == by]
    by <- list(by)
    names(by) <- by_name
  } else {
    stop("syntax error in ", sQuote("by"), call. = FALSE)
  }

  if (length(window_calls(by, dbi_connection(x)))) {
    stop("Aggregate and window functions are not allowed in ", sQuote("by"))
  }
  by
}



handle_j <- function(x, j, by) {
  if (is.null(j)) {
    return(x)
  }

  switch(is_call_to(j),
    "not a call" = {
      stop(sQuote("j"), " is not a call", call. = FALSE)
    },

    "list" = {
      j <- as.list(j[-1])
    },

    "n" = {
      j <- list(N = call("n"))
    },

    {
      if (!is.null(by)) {
        j <- list(j)
      } else {
        stop(sQuote("j"), " is not a call to ", sQuote("list"), call. = FALSE)
      }
    }
  )

  by <- handle_by(x, by)

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
