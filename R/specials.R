unsupported <- function(sym) {
  eval(bquote(function(e, dbi_table, specials, env) {
    stop("the 'data.table' special symbol '", .(sym), "' is not supported by ",
         "'dbi.table'", call. = FALSE)
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



special_in <- function(e, dbi_table, specials, env) {
  e[[1]] <- as.name("%in%")
  e[[2]] <- sub_lang(e[[2]], dbi_table, specials, env)
  e[[3]] <- if_allowed_mode(eval(e[[3]], envir = env))
  e
}



special_local <- function(e, dbi_table, specials, env) {
  eval(e[[2L]], NULL, env)
}



special_not <- function(e, dbi_table, specials, env) {
  call("!", sub_lang(e[[2L]], dbi_table, enclos = env))
}



special_like <- function(e, dbi_table, specials, env) {
  e[[1L]] <- as.name("%LIKE%")
  rhs <- eval(e[[3L]], env)
  if (length(rhs) != 1L) {
    stop("the right-hand side of '%like%' did not evaluate to a scalar")
  }
  e[[3L]] <- paste0("%", rhs, "%")
  e
}



special_LIKE <- function(e, dbi_table, specials, env) {
  rhs <- eval(e[[3L]], env)
  if (length(rhs) != 1L) {
    stop("the right-hand side of '%LIKE%' did not evaluate to a scalar")
  }
  e[[3L]] <- rhs
  e
}



add_special <- function(symbol, fun = unsupported(symbol)) {
  stopifnot(is.character(symbol) && (length(symbol) == 1L) && nchar(symbol) > 0)
  session$special_symbols[[symbol]] <- fun
  invisible()
}
