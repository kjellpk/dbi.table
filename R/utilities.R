reconnect <- function(what) {
  if (is.dbi.table(what)) {
    what <- attr(what, "conn")
  }

  if (is.environment(what) && !is.null(recon <- what$.recon)) {
    what$.dbi <- recon()
    warning("reconnecting to database...")

    #' @importFrom DBI dbIsValid
    return(dbIsValid(what$dbi))
  }

  FALSE
}



db_short_name <- function(conn, pkg = FALSE) {
  #' @importFrom DBI dbGetInfo
  n <- sub("([^.]+)\\.[[:alnum:]]+$", "\\1", basename(dbGetInfo(conn)$dbname))

  if (pkg && !is.null(pkg <- attr(class(conn), "package", exact = TRUE))) {
    n <- paste(pkg, n, sep = ":")
  }

  n
}



paren <- function(x) {
  paste0("(", x, ")")
}



#' See SQL
#'
#' @description View a \code{\link{dbi.table}} as a SQL query.
#'
#' @param x a \code{\link{dbi.table}}.
#' @export
csql <- function(x) {
  cat(paste0("\n", write_sql(x), "\n\n"))
}



ws <- function(n) {
  sapply(n, function(u) paste(rep(" ", u), collapse = ""))
}



pad_left <- function(x, width = 6) {
  fw <- sapply(strsplit(x, split = "[ \t\r\n]"), function(u) u[1])
  paste0(ws(width - nchar(fw)), x)
}



e <- new.env()

unique_table_name <- function(pre = "X") {
  if (is.null(e$u_counter)) {
    e$u_counter <- 0L
  }

  paste0(pre, (e$u_counter <- 1L + e$u_counter))
}



split_by_id <- function(x, idcols) {
  keep <- setdiff(names(x), idcols)
  x <- split(x, f = x[, idcols])

  ids <- unname(lapply(x, `[`, i = 1, j = idcols, drop = FALSE))
  #' @importFrom DBI Id
  ids <- lapply(ids, function(u) Id(unlist(as.list(u))))

  cols <- list()
  for (k in keep) {
    u <- unname(lapply(x, `[[`, i = k))
    cols[[k]] <- u[!is.na(u) & nchar(u) > 0]
  }

  do.call(data.frame, c(list(id = I(ids)), lapply(cols, I)))
}
