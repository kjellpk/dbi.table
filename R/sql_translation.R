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
