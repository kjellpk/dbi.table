#' @rawNamespace S3method(data.table::as.data.table,dbi.table,as_data_table)
as_data_table <- function(x, ...) {
  data.table::setDT(as.data.frame(x, ...))[]
}
