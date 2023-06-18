# Managed DBI connections
dbi_connections <- new.env()
connection_hashes <- new.env()
reconnection_functions <- new.env()



hash_connection <- function(conn) {
  #' @importFrom utils capture.output
  #' @importFrom methods show
  substring(o <- capture.output(show(conn@ptr)), 11, nchar(o) - 1)
}



is_hash <- function(x) {
  (length(x) == 1L) && is.character(x) && (x %in% ls(connection_hashes))
}



register_connection <- function(conn) {
  if (is.function(conn)) {
    conn_fun <- conn
    conn <- conn()
  } else {
    conn_fun <- NULL
  }

  #' @importFrom methods is
  if (!is(conn, "DBIConnection")) {
    return(NULL)
  }

  h <- hash_connection(conn)

  if (is.null(get_connection_from_hash(h))) {
    assign(h, h, envir = connection_hashes)
    assign(h, conn, envir = dbi_connections)
    if (!is.null(conn_fun)) {
      assign(h, conn_fun, envir = reconnection_functions)
    }
  }

  h
}



get_active_hash <- function(hash) {
  connection_hashes[[hash]]
}



get_connection_from_hash <- function(hash) {
  if (is.null(active_hash <- get_active_hash(hash))) {
    return(NULL)
  }

  dbi_connections[[active_hash]]
}



reconnect <- function(hash) {
  hash <- get_active_hash(hash)

  if (is.null(fun <- reconnection_functions[[hash]])) {
    stop("DBI connection not valid")
  } else {
    message("reestablishing connection to database...")
    new_hash <- register_connection(fun)

    for (h in ls(connection_hashes)) {
      if (connection_hashes[[h]] == hash) connection_hashes[[h]] <- new_hash
    }

    try(dbi_connections[[hash]], silent = TRUE)
    try(rm(list = hash, envir = dbi_connections), silent = TRUE)
    try(rm(list = hash, envir = reconnection_functions), silent = TRUE)
  }

  invisible()
}



connections <- function() {
  x <- unlist(eapply(connection_hashes, as.character))
  #' @importFrom DBI dbIsValid
  valid <- vapply(x, function(u) dbIsValid(dbi_connections[[u]]), FALSE)
  reconnectable <- vapply(x, function(u) !is.null(reconnection_functions[[u]]),
                          FALSE)
  data.table(hash = names(x),
            active_hash = unname(x),
            valid = valid,
            reconnectable = reconnectable)
}
