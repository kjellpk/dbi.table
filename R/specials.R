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

  e[-1] <- lapply(e[-1], sub_lang, envir = dbi_table,
                  specials = specials, enclos = env)

  e
}



special_colon <- function(e, dbi_table, specials, env) {
  if (is.name(lhs <- e[[2]])) {
    lhs <- match(as.character(lhs), names(dbi_table))
  }

  if (is.name(rhs <- e[[3]])) {
    rhs <- match(as.character(rhs), names(dbi_table))
  }

  if (!all(c(lhs, rhs) %in% seq_along(dbi_table))) {
    stop("problem in :")
  }

  cl <- sapply(names(dbi_table)[lhs:rhs], as.name, simplify = FALSE)
  as.call(c(list(as.name("list")), cl))
}



special_c <- function(e, dbi_table, specials, env) {
  e_vars <- all.vars(e)

  if (!all(substring(e_vars, 1, 2) == "..")) {
    stop("handle case where 'name' is not in dbi.table")
  }

  if (length(e_vars)) {
    nl <- lapply(substring(e_vars, 3), as.name)
    names(nl) <- e_vars
    e <- sub_lang(e, envir = nl, specials = NULL)
  }

  if (is.character(e <- eval(e, envir = env))) {
    e <- match(e, names(dbi_table))
  }

  cl <- sapply(names(dbi_table)[e], as.name, simplify = FALSE)
  as.call(c(list(as.name("list")), cl))
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
