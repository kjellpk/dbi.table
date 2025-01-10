#' @exportS3Method data.table::as.data.table
as.data.table.dbi.table <- function(x, ...) {
  data.table::setDT(as.data.frame(x, ...))[]
}
