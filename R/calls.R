sub_lang <- function(e, cdefs) {
  if (is.null(e)) {
    return(NULL)
  } else if (is.name(e)) {
    if ((ce <- as.character(e)) %in% names(cdefs)) {
      e <- cdefs[[ce]]
    }
  } else if (is.call(e)) {
    for (i in seq_along(e)[-1]) {
      e[[i]] <- sub_lang(e[[i]], cdefs)
    }
  }
  e
}



get_names <- function(e, symbols = new.env(), level = 1L) {
  if (is.name(e)) {
    assign(as.character(e), 1, envir = symbols)
  } else if (is.call(e)) {
    lapply(as.list(e[-1]), get_names, symbols = symbols, level = level + 1L)
  }

  if (level == 1L) ls(symbols) else NULL
}



prepare_call <- function(e, vars, env, data = NULL) {
  if (is.null(data)) {
    data <- as.list(logical(length(var_names <- names(vars))))
    names(data) <- var_names
    data <- data[unique(names(data))]

    #' @importFrom dbplyr lazy_frame
    data <- do.call(lazy_frame, data)
  }

  #' @importFrom dbplyr partial_eval
  sub_lang(partial_eval(e, data = data, env = env), cdefs = vars)
}



prepare_calls <- function(calls, vars, env) {
  data <- as.list(logical(length(var_names <- names(vars))))
  names(data) <- var_names
  data <- data[unique(names(data))]

  #' @importFrom dbplyr lazy_frame
  data <- do.call(lazy_frame, data)

  lapply(calls, prepare_call, vars = vars, env = env, data = data)
}



handy_andy <- function(l) {
  if (!is.list(l) || !length(l)) {
    return(NULL)
  }

  names(l) <- paste0("x", seq_along(l))
  sub_lang(str2lang(paste(paren(names(l)), collapse = "&")), cdefs = l)
}



OVER_PATTERN <- "\\)\\s*OVER\\s*\\("

window_calls <- function(x, conn) {
  suppressWarnings(
    #' @importFrom dbplyr translate_sql_
    grepl(OVER_PATTERN, translate_sql_(x, con = conn, window = TRUE))
  )
}



AGGREGATE_FUNCTIONS <- c("mean", "sum", "min", "max", "n")

call_can_aggregate <- can_aggregate <- function(e) {
  if (is.atomic(e) && length(e) == 1L) {
    return(TRUE)
  }

  if (is.name(e)) {
    return(FALSE)
  }

  if (is.call(e)) {
    if (as.character(e[[1]]) %in% AGGREGATE_FUNCTIONS) {
      return(TRUE)
    } else {
      return(all(sapply(as.list(e)[-1], call_can_aggregate)))
    }
  }

  stop(sQuote("e"), " is not language or atomic")
}



calls_can_aggregate <- function(calls) {
  vapply(calls, call_can_aggregate, FALSE)
}