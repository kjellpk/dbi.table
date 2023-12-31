unsupported <- function(sym) {
  eval(bquote(function(e, dbi_table, specials, env) {
    stop("the ", sQuote("data.table"), " special symbol ", sQuote(.(sym)),
         " is not supported by ", sQuote("dbi.table"), call. = FALSE)
  }))
}



special_list <- function(e, dbi_table, specials, env) {
  e[[1]] <- as.name("list")

  if (is.null(nm <- names(e))) {
    nm <- character(length(e))
  }

  if (any(idx <- (nchar(nm) == 0L) & vapply(e, is.name, FALSE))) {
    tmp <- vapply(e[idx], as.character, "")
    is_spec <- tmp %in% names(session$special_symbols)
    tmp[is_spec] <- ifelse(substring(tmp[is_spec], 1, 1) == ".",
                           substring(tmp[is_spec], 2),
                           tmp[is_spec])
    nm[idx] <- tmp
    names(e) <- nm
  }

  e[-1] <- lapply(e[-1], sub_lang, dbi_table = dbi_table,
                  specials = specials, env = env)

  e
}



add_special <- function(symbol, fun = unsupported(symbol)) {
  stopifnot(is.character(symbol) && (length(symbol) == 1L) && nchar(symbol) > 0)
  session$special_symbols[[symbol]] <- fun
  invisible()
}



is_special <- function(e) {
  is.name(e) && !is.null(session$special_symbols[[as.character(e)]])
}
