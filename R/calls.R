sub_lang <- function(e, remotes = NULL, locals = NULL) {
  if (is.null(e)) {
    return(NULL)
  }

  if (!is.null(remotes) &&
        all(!(all.vars(e) %in% names(remotes))) &&
        all(!(all.vars(e) %in% names(session$special_symbols))) &&
        all(!(all.names(e) %in% names(session$special_functions)))) {
    v <- eval(e, envir = locals)

    if (!is.vector(v) || length(v) != 1L) {
      stop("local variable is not scalar")
    }

    return(use_integer(v))
  }


  e <- reinplace_special(e)

  if (is.name(e) && !is.null(r <- remotes[[as.character(e)]])) {
    return(r)
  }

  if (is.call(e)) {
    if (as.character(e[[1]]) == "list") {
      if (is.null(nm <- names(e))) {
        nm <- character(length(e))
      }

      idx <- (nchar(nm) == 0L) & vapply(e, is.name, FALSE)

      tmp <- vapply(e[idx], as.character, "")
      is_spec <- tmp %in% names(session$special_symbols)
      tmp[is_spec] <- substring(tmp[is_spec], 2)

      nm[idx] <- tmp
      names(e) <- nm
    }

    if (as.character(e[[1]]) == ":=") {
      lhs <- e[[2]]
      e[[2]] <- NULL
      stopifnot(is.name(lhs))
      names(e)[2] <- as.character(lhs)
      e[[2]] <- sub_lang(e[[2]], remotes = remotes, locals = locals)
    } else {
      e[-1] <- lapply(e[-1], sub_lang, remotes = remotes, locals = locals)
    }

    return(e)
  }

  browser()
  stop("should never get here")

  NULL
}



use_integer <- function(x) {
  if (is.double(x)) {
    if (max(abs(x - as.integer(x))) < .Machine$double.eps) {
      x <- as.integer(x)
    }
  }

  x
}



handy_andy <- function(l) {
  if (!is.list(l) || !length(l)) {
    return(NULL)
  }

  names(l) <- paste0("x", seq_along(l))
  sub_lang(str2lang(paste(paren(names(l)), collapse = "&")), remotes = l)
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
      return(all(vapply(as.list(e)[-1], call_can_aggregate, FALSE)))
    }
  }

  stop(sQuote("e"), " is not language or atomic")
}



calls_can_aggregate <- function(calls) {
  vapply(calls, call_can_aggregate, FALSE)
}