sub_lang <- function(e, envir = NULL, specials = session$special_symbols,
                     enclos = NULL) {
  if (is.null(e)) {
    return(NULL)
  }

  if (is.name(e)) {
    e_char <- as.character(e)

    if (nchar(e_char) < 1L) {
      return(NULL)
    }

    if (!is.null(envir[[e_char]])) {
      return(eval(e, envir, NULL))
    }

    if (!is.null(specials[[e_char]])) {
      return(eval(e, specials, NULL)(e, envir, specials, enclos))
    }

    if (is.call(e <- eval(e, NULL, enclos))) {
      return(sub_lang(e, envir, specials, enclos))
    }

    if (is.dbi.table(e) || is.data.frame(e)) {
      return(e)
    }

    return(if_scalar(e))
  }

  if (is.call(e)) {
    if (!is.null(specials[[as.character(e[[1L]])]])) {
      return(eval(e[[1L]], specials, NULL)(e, envir, specials, enclos))
    }

    if (!any(all.vars(e) %in% names(envir))) {
      ee <- eval(e, NULL, enclos)

      if (is.data.frame(ee) || is.dbi.table(ee)) {
        return(ee)
      }
    }

    e[-1] <- lapply(e[-1L], sub_lang, envir = envir,
                    specials = specials, enclos = enclos)

    return(e)
  }

  if (is.list(e) && all(vapply(e, is_language, FALSE))) {
    return(lapply(e, sub_lang, envir = envir, specials = specials,
                  enclos = enclos))
  }

  if_scalar(e)
}



names_list <- function(x, names.out = NULL) {
  if (is.dbi.table(x)) {
    x <- names(x)
  }
  x <- as.character(x)
  if (is.null(names.out)) {
    names.out <- x
  } else {
    names.out <- as.character(names.out)
  }
  names(x) <- names.out
  sapply(x, as.name, simplify = FALSE)
}



use_integer <- function(x) {
  if (is.numeric(x) && !any(class(x) %in% c("POSIXct", "Date"))) {
    if (max(abs(x - (rx <- round(x, digits = 0L)))) < .Machine$double.eps) {
      if (max(abs(rx)) > .Machine$integer.max) {
        x <- bit64::as.integer64(rx)
      } else {
        x <- as.integer(rx)
      }
    }
  }

  x
}



SQL_MODES <- c("numeric", "character", "logical")

if_allowed_mode <- function(x) {
  if (!is.atomic(x) || !(mode(x) %in% SQL_MODES)) {
    stop("'x' is not atomic", call. = FALSE)
  }
  use_integer(x)
}



if_scalar <- function(x) {
  if (length(x <- if_allowed_mode(x)) != 1L) {
    stop("'x' is not scalar", call. = FALSE)
  }

  x
}



`%is_call_to%` <- function(cl, choices) {
  if (is.call(cl)) {
    return(as.character(cl[[1L]]) %in% as.character(choices))
  }

  FALSE
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
    grep(OVER_PATTERN, translate_sql_(x, con = conn, window = TRUE))
  )
}



AGGREGATE_FUNCTIONS <- c("mean", "sum", "min", "max", "sd", "var", "n")

call_can_aggregate <- function(e) {
  if (is_scalar_atomic(e)) {
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

  stop("'e' is not language or atomic")
}



calls_can_aggregate <- function(calls) {
  vapply(calls, call_can_aggregate, FALSE)
}
