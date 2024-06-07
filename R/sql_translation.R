sub_db_identifier <- function(e, conn, fields) {
  qname <- DBI::dbQuoteIdentifier(conn, fields$internal_name)

  qfield <- paste(DBI::dbQuoteIdentifier(conn, fields$id),
                  DBI::dbQuoteIdentifier(conn, fields$field),
                  sep = ".")

  stringi::stri_replace_all_fixed(e, qname, qfield, vectorize_all = FALSE)
}



translate_sql_ <- function(dots, con, vars_group = NULL, vars_order = NULL,
                           vars_frame = NULL, window = TRUE, context = list()) {
  stopifnot(is.list(dots) || is.null(dots))

  dots <- lapply(dots, rlang::as_quosure, env = emptyenv())

  dbplyr::translate_sql_(dots = dots, con = con, vars_group = vars_group,
                         vars_order = vars_order, vars_frame = vars_frame,
                         window = window, context = context)
}
