unsupported <- function(sym) {
  msg <- paste("the", sQuote("data.table"), "special symbol", sQuote(sym),
               "is not supported by", sQuote("dbi.table"))
  simpleError(msg)
}



add_special <- function(key, value = unsupported(key),
                        map = c("name", "call")) {
  map <- match.arg(map)
  stopifnot(is.character(key) && (length(key) == 1L) && nchar(key) > 0)
  stopifnot(is.language(value) || inherits(value, "simpleError"))

  switch(map,
    name = {session$special_symbols[[key]] <- value},
    call = {session$special_functions[[key]] <- value}
  )

  invisible()
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
    if (!is.null(m <- session$special_functions[[as.character(e[[1]])]])) {
      if (inherits(m, "simpleError")) {
        stop(m)
      }

      e[[1]] <- m
    }
  }

  e
}
