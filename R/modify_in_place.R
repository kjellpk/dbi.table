modify_in_place <- function(x, x_sub, envir, value) {
  object_name <- class(x)[[1L]]

  if (is.null(sym <- peel_to_first(x_sub)) ||
        is.null(envir <- find_env(sym, envir)) ||
        !identical(eval(x_sub, envir), x)) {
    warning(object_name, " ", sQuote(format(x_sub)),
            " could not be modified in place", call. = FALSE)
    return(NULL)
  }

  shim <- new.env(parent = parent.env(envir))
  parent <- parent.env(envir)
  parent.env(envir) <- shim
  on.exit(parent.env(envir) <- parent)

  nm <- "++++++++"
  while (exists(nm, envir)) {
    nm <- paste0(nm, sample(0:9, 1L))
  }

  assign(nm, value, shim)

  expr <- parse(text = "y <- x")[[1L]]
  expr[[2L]] <- x_sub
  expr[[3L]] <- as.symbol(nm)

  if (inherits(res <- stry(eval(expr, envir)), "try-error")) {
    warning(attr(res, "condition")$message, call. = FALSE)
  }

  NULL
}



peel_to_first <- function(expr) {
  if (is.symbol(expr)) {
    return(as.character(expr))
  }

  if (expr %is_call_to% c("$", "[[", "[") && !is.call(expr[[3L]])) {
    return(peel_to_first(expr[[2L]]))
  }

  NULL
}



find_env <- function(sym, env) {
  if (identical(env, emptyenv())) {
    return(NULL)
  }

  if (exists(sym, env, inherits = FALSE)) {
    return(env)
  }

  find_env(sym, parent.env(env))
}
