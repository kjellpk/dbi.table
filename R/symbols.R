unsupported <- function(sym) {
  msg <- paste("the", sQuote("data.table"), "special symbol", sQuote(sym),
               "is not supported by", sQuote("dbi.table"))
  simpleError(msg)
}



add_special <- function(key, value = unsupported(key)) {
  stopifnot(is.character(key) && (length(key) == 1L) && nchar(key) > 0)
  stopifnot(is.language(value) || inherits(value, "simpleError"))

  session$special_symbols[[key]] <- value

  invisible()
}



is_special <- function(e) {
  is.name(e) && !is.null(session$special_symbols[[as.character(e)]])
}



reinplace_special <- function(e) {
  if (is.name(e)) {
    if (!is.null(m <- session$special_symbols[[as.character(e)]])) {
      if (inherits(m, "simpleError")) {
        stop(m)
      }

      e <- m
    }
  } else if (is.call(e)) {
    if (!is.null(m <- session$special_symbols[[as.character(e[[1]])]])) {
      if (inherits(m, "simpleError")) {
        stop(m)
      }

      e[[1]] <- m
    }
  }

  e
}
