as_data_table <- function(x, ...) {
  data.table::setDT(as.data.frame(x, ...))[]
}
