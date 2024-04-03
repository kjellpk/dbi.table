sub_db_identifier <- function(e, conn, fields) {
  #' @importFrom DBI dbQuoteIdentifier
  qname <- dbQuoteIdentifier(conn, fields$internal_name)

  #' @importFrom DBI dbQuoteIdentifier
  qfield <- paste(dbQuoteIdentifier(conn, fields$id),
                  dbQuoteIdentifier(conn, fields$field),
                  sep = ".")

  #' @importFrom stringi stri_replace_all_fixed
  stri_replace_all_fixed(e, qname, qfield, vectorize_all = FALSE)
}



dbplyr_translate_sql_ <- dbplyr::translate_sql_



translate_sql_ <- function(dots, con, vars_group = NULL, vars_order = NULL,
                           vars_frame = NULL, window = TRUE, context = list()) {
  stopifnot(is.list(dots) || is.null(dots))

  #' @importFrom rlang as_quosure
  dots <- lapply(dots, as_quosure, env = emptyenv())

  dbplyr_translate_sql_(dots = dots, con = con, vars_group = vars_group,
                        vars_order = vars_order, vars_frame = vars_frame,
                        window = window, context = context)
}



# Need to import something from dbplyr to avoid R CMD check note.
dummy <- function() {
  #' @importFrom dbplyr test_register_src
  test_register_src("name", "src")
}
