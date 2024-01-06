triage_brackets <- function(x, i, j, by, env = NULL, x_sub = NULL) {
  if (is.call(j) && j[[1]] == ":=") {
    return(handle_colon_equal(x, i, j, by, env, x_sub))
  }

  if (is.null(j) && !is.null(by)) {
    stop("cannot handle ", sQuote("by"), " when ", sQuote("j"),
         " is missing or ", sQuote("NULL"))
  }

  x <- handle_i(x, i)

  if (!dbi_table_is_simple(x)) {
    x <- as_cte(x)
  }

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

  if (is.call(by) && as.character(by[[1]]) == "list") {
    by <- as.list(by[-1])
  } else {
    stop("by is not a call to list")
  }

  if (length(window_calls(by, get_connection(x)))) {
    stop("Aggregate and window functions are not allowed in ", sQuote("by"))
  }

  by
}



handle_j <- function(x, j, by) {
  if (is.null(j)) {
    return(x)
  }

  if (is.call(j) && as.character(j[[1]]) == "list") {
    j <- as.list(j[-1])
  } else {
    stop("j is not a call to list")
  }

  by <- handle_by(x, by)
  a <- attributes(x)

  if (all(calls_can_aggregate(j))) {
    a$group_by <- by
  } else {
    j <- handle_over(x, j, by, a$order_by) 
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



handle_over <- function(x, j, partition, order) {
  over <- list(partition_by = unname(partition), order_by = unname(order))

  for (k in window_calls(j, get_connection(x))) {
    attr(j[[k]], "over") <- over
  }

  j
}



handle_colon_equal <- function(x, i, j, by, env, x_sub) {
  j <- as.list(j[-1])
  a <- attributes(x)
  a$names <- NULL
  x <- c(x)
  x[names(j)] <- j
  a$names <- names(x)
  attributes(x) <- a

  if (is.name(x_sub)) {
    # try to handle assignment to environment and list too
    assign(as.character(x_sub), x, envir = env)
  }

  #invisible doesn't work - use data.table's workaround
  session$print <- address(x)
  x
}