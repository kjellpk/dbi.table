sub_lang <- function(e, envir = NULL, specials = session$special_symbols,
                     enclos = NULL) {
  if (is.null(e)) {
    return(NULL)
  }

  if (is.name(e)) {
    e_char <- as.character(e)

    if (nchar(e_char) < 1) {
      return(NULL)
    }

    if (!is.null(envir) && (e_char %chin% names(envir))) {
      return(c(envir)[[e_char]])
    }

    if (!is.null(specials) && (e_char %chin% names(specials))) {
      return(specials[[e_char]](e, envir, specials, enclos))
    }

    if (!is.null(enclos) && !is.null(enclos[[e_char]])) {
      return(if_scalar(eval(e, envir = enclos)))
    }

    stop("symbol ", sQuote(e), " not found")
  }

  if (is.call(e)) {
    if (!is.null(specials) &&
          ((ec <- as.character(e[[1]])) %chin% names(specials))) {
      return(specials[[ec]](e, envir, specials, enclos))
    }

    e[-1] <- lapply(e[-1], sub_lang, envir = envir,
                    specials = specials, enclos = enclos)

    return(e)
  }

  if_scalar(e)
}



use_integer <- function(x) {
  if (is.double(x)) {
    if (max(abs(x - as.integer(x))) < .Machine$double.eps) {
      x <- as.integer(x)
    }
  }

  x
}



if_scalar <- function(x) {
  if (!(mode(x) %chin% c("numeric", "character", "logical")) ||
        length(x) != 1L) {
    stop("only scalar symbols can be substituted into calls")
  }
  use_integer(x)
}



is_call_to <- function(cl) {
  if (!is.call(cl)) {
    return(paste(deparse1(substitute(cl)), "is not a call"))
  }

  as.character(cl[[1]])
}



is_scalar_atomic <- function(x) {
  is.atomic(x) && (length(x) == 1L)
}



is_language <- function(x) {
  is.language(x) || is_scalar_atomic(x)
}



is_list_of <- function(FUN, x) {
  is.list(x) && all(vapply(x, FUN, FALSE))
}



handy_andy <- function(x) {
  if (!is_list_of(is_language, x)) {
    return(x)
  }

  if (length(x) > 1L) {
    names(x) <- paste0("x", seq_along(x))
    sub_lang(str2lang(paste(paren(names(x)), collapse = "&")),
             envir = x, specials = NULL)
  } else {
    x[[1]]
  }
}



OVER_PATTERN <- "\\)\\s*OVER\\s*\\("

window_calls <- function(x, conn) {
  suppressWarnings(
    #' @importFrom dbplyr translate_sql_
    grep(OVER_PATTERN, translate_sql_(x, con = conn, window = TRUE))
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
    if (as.character(e[[1]]) %chin% AGGREGATE_FUNCTIONS) {
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