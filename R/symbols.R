special_symbols <- new.env()
special_functions <- new.env()



unsupported <- function(sym) {
  msg <- paste("the", sQuote("data.table"), "special symbol", sQuote(sym),
               "is not supported by", sQuote("dbi.table"))
  simpleError(msg)
}



add_special <- function(key, value = unsupported(key), map) {
  stopifnot(is.character(key) && (length(key) == 1L) && nchar(key) > 0)
  stopifnot(is.language(value) || inherits(value, "simpleError"))

  map[[key]] <- value
}



try_map <- function(key, map) {
  if (is.null(map)) {
    return(NULL)
  }

  key <- as.character(key)[1]

  if (inherits(value <- map[[key]], "simpleError")) {
    stop(value)
  }

  value
}
