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

  lapply(e[-1], sub_lang, envir = dbi_table, specials = specials, enclos = env)
}



special_colon_equals <- function(e, dbi_table, specials, env) {
  if (length(e) == 2L && !is.null(names(e[[2]]))) {
    e[2] <- sub_lang(e[2], envir = dbi_table, specials = specials,
                     enclos = env)
    return(e)
  }

  rhs <- sub_lang(e[[3]], envir = dbi_table, specials = specials, enclos = env)

  if (is_call_to(rhs) == "list") {
    rhs[[1]] <- as.name(":=")
  } else {
    rhs <- call(":=", rhs)
  }

  if (is.call(nm <- e[[2]])) {
    lhs <- eval(nm, env)
  } else if (is.name(nm)) {
    lhs <- as.character(nm)
  }

  if (is.null(nms <- names(rhs))) {
    nms <- character(length(rhs))
  }

  nms[-1] <- lhs
  names(rhs) <- nms
  rhs
}



add_special <- function(symbol, fun = unsupported(symbol)) {
  stopifnot(is.character(symbol) && (length(symbol) == 1L) && nchar(symbol) > 0)
  session$special_symbols[[symbol]] <- fun
  invisible()
}
