add_special <- function(symbol, fun = unsupported(symbol)) {
  stopifnot(is.character(symbol) && (length(symbol) == 1L) && nchar(symbol) > 0)
  session$special_symbols[[symbol]] <- fun
  invisible()
}



unsupported <- function(sym) {
  eval(bquote(function(e, dbi_table, specials, env) {
    stop("the 'data.table' special symbol '", .(sym), "' is not supported by ",
         "'dbi.table'", call. = FALSE)
  }))
}



special_in <- function(e, dbi_table, specials, env) {
  e[[1]] <- as.name("%in%")
  e[[2]] <- sub_lang(e[[2]], dbi_table, specials, env)
  e[[3]] <- if_allowed_mode(eval(e[[3]], envir = env))
  e
}



special_notin <- function(e, dbi_table, specials, env) {
  e[[1]] <- as.name("%in%")
  e <- as.call(list(as.name("!"), e))
  sub_lang(e, dbi_table, specials, env)
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



special_order <- function(e, dbi_table, specials, env) {
  e[[1L]] <- as.name("order")
  sub_lang(e, dbi_table, enclos = env)
}
