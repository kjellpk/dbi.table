#' @rawNamespace S3method(data.table::as.data.table,dbi.table,as_data_table)
as_data_table <- function(x, ...) {
  if (is.null(n <- list(...)$n)) {
    n <- getOption("dbitable.max.fetch", 10000L)
  } else {
    n <- as.integer(n)
  }

  x_key <- get_key(x)
  x <- as.data.frame(x, n = n, strict = n > 0L)
  data.table::setDT(x, key = x_key)[]
}
