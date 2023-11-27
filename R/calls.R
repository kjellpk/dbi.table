sub_lang <- function(e, remotes = NULL, locals = NULL) {
  if (is.null(e)) {
    return(NULL)
  } else if (is.name(e)) {
    if (!is.null(u <- try_map(e, special_symbols))) {
      e <- u
    } else if (!is.null(r <- try_map(e, remotes))) {
      e <- r
    } else if (!is.null(l <- try_map(e, locals)))  {
      if (is.vector(l) && (length(l) == 1L)) {
        e <- l
      } else {
        stop("only scalar local variables are supported at this time")
      }
    } else {
      stop("symbol ", sQuote(e), " not found")
    }
  } else if (is.call(e)) {
    if (!is.null(r <- try_map(e[[1]], special_functions))) {
      e[[1]] <- r
    }

    if (as.character(e[[1]]) == "list") {
      if (is.null(nm <- names(e[-1]))) {
        names(e[-1]) <- paste0("V", seq_along(e[-1]))
      } else if (any(nx <- (nchar(nm) == 0L))) {
        v <- paste0("V", seq_along(e[-1]))
        en <- vapply(e[-1], is.name, FALSE)
        v[en] <- vapply(e[-1][en], as.character, "")
        nm[nx] <- v[nx]
        names(e)[-1] <- nm
      }
    }

    e[-1] <- lapply(e[-1], sub_lang, remotes = remotes, locals = locals)
  }
  e
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
      return(all(sapply(as.list(e)[-1], call_can_aggregate)))
    }
  }

  stop(sQuote("e"), " is not language or atomic")
}



calls_can_aggregate <- function(calls) {
  vapply(calls, call_can_aggregate, FALSE)
}