specials <- new.env(parent = emptyenv())
assign("parent", new.env(parent = emptyenv()), specials)


specials$parent[[".N"]] <- function(e, dbi_table, specials, env) call("n")


for (sym in c(".SD", ".BY", ".I", ".GRP", ".NGRP", ".EACHI")) {
  specials$parent[[sym]] <- eval(bquote(
    function(e, dbi_table, specials, env)
      stop("the 'data.table' special symbol '", .(sym),
           "' is not supported by 'dbi.table'", call. = FALSE)))
}


specials$parent[["%in%"]] <- function(e, dbi_table, specials, env) {
  e[[1]] <- as.name("%in%")
  e[[2]] <- sub_lang(e[[2]], dbi_table, specials, env)
  e[[3]] <- if_allowed_mode(eval(e[[3]], envir = env))
  e
}


specials$parent[["%chin%"]] <- specials$parent[["%in%"]]


specials$parent[["%notin%"]] <- function(e, dbi_table, specials, env) {
  e[[1]] <- as.name("%in%")
  e <- as.call(list(as.name("!"), e))
  sub_lang(e, dbi_table, specials, env)
}


specials$parent[["local"]] <- function(e, dbi_table, specials, env) {
  eval(e[[2L]], NULL, env)
}


specials$parent[["!"]] <- function(e, dbi_table, specials, env) {
  call("!", sub_lang(e[[2L]], dbi_table, specials, env))
}


specials$parent[["%like%"]]  <- function(e, dbi_table, specials, env) {
  e[[1L]] <- as.name("%LIKE%")
  rhs <- eval(e[[3L]], env)
  if (length(rhs) != 1L) {
    stop("the right-hand side of '%like%' did not evaluate to a scalar")
  }
  e[[3L]] <- paste0("%", rhs, "%")
  e
}


specials$parent[["chorder"]] <- function(e, dbi_table, specials, env) {
  e[[1L]] <- as.name("order")
  sub_lang(e, dbi_table, specials, env)
}


specials$parent[["shift"]] <- function(e, dbi_table, specials, env) {
  shift_args <- function(x, n = 1L, fill = NA, type = "lag",
                         give.names = FALSE) {
    list(n = n, fill = fill, type = type, give.names = give.names)
  }

  sargs <- e
  sargs[[1L]] <- as.name("shift_args")
  sargs <- eval(sargs)

  n <- as.integer(sargs$n)
  fill <- sargs$fill
  type <- match.arg(sargs$type, choices = c("lag", "lead", "shift", "cyclic"))

  if (type == "cyclic") {
    stop("'type = cyclic' is not supported by dbi.table", call. = FALSE)
  }

  if (type == "shift") {
    type <- "lag"
  }

  if (n < 0L) {
    n <- abs(n)
    type <- ifelse(type == "lag", "lead", "lag")
  }

  # Don't call 'call' here directly - R CMD check throws a partial match warning
  e <- as.call(list(as.name(type), x = e[[2L]], n = n, default = fill))
  sub_lang(e, dbi_table, specials, env)
}


specials$parent[["$"]] <- function(e, dbi_table, specials, env) {
  if_scalar(eval(e, env))
}


specials$parent[["[["]] <- function(e, dbi_table, specials, env) {
  if_scalar(eval(e, env))
}


specials$parent[["["]] <- function(e, dbi_table, specials, env) {
  if_scalar(unname(unlist(eval(e, env))))
}


get_specials <- function(conn) {
  if (is.dbi.table(conn)) {
    conn <- get_connection(conn)
  }

  if (is_dbi_catalog(conn)) {
    return(get("./specials", conn))
  }

  if (is.null(s <- get0(nm <- class(conn)[[1L]], specials))) {
    s <- new_specials_env_from_conn(conn)
    parent.env(s) <- specials$parent
    assign(nm, s, specials)
  }

  s
}


new_specials_env_from_conn <- function(conn) {
  UseMethod("new_specials_env_from_conn")
}



#' @rawNamespace S3method(new_specials_env_from_conn,default,new_specials_env_from_conn_default)
new_specials_env_from_conn_default <- function(conn) {
  new.env(parent = emptyenv())
}
