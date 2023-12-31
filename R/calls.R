sub_lang <- function(e, dbi_table = NULL, specials = session$special_symbols,
                     env = NULL) {
  if (is.null(e)) {
    return(NULL)
  }

  if (is.name(e)) {
    e_char <- as.character(e)

    if (nchar(e_char) < 1) {
      return(NULL)
    }

    if (!is.null(dbi_table) && (e_char %in% names(dbi_table))) {
      return(c(dbi_table)[[e_char]])
    }

    if (!is.null(specials) && (e_char %in% names(specials))) {
      return(specials[[e_char]](e, dbi_table, specials, env))
    }

    if (!is.null(env) && !is.null(env[[e_char]])) {
      return(if_scalar(eval(e, envir = env)))
    }

    stop("symbol ", sQuote(e), " not found")
  }

  if (is.call(e)) {
    if (!is.null(specials) &&
          ((ec <- as.character(e[[1]])) %in% names(specials))) {
      return(specials[[ec]](e, dbi_table, specials, env))
    }

    e[-1] <- lapply(e[-1], sub_lang, dbi_table = dbi_table,
                    specials = specials, env = env)

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
  if (!(mode(x) %in% c("numeric", "character", "logical")) || length(x) != 1L) {
    stop("only scalar symbols can be substituted into calls")
  }
  use_integer(x)
}


handy_andy <- function(l) {
  if (!is.list(l) || !length(l)) {
    return(NULL)
  }

  names(l) <- paste0("x", seq_along(l))
  sub_lang(str2lang(paste(paren(names(l)), collapse = "&")),
           dbi_table = l, specials = NULL, env = NULL)
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