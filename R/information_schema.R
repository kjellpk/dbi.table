get_information_schema <- function(x) {
  if (is_dbi_catalog(conn <- get_connection(x))) {
    return(conn$information_schema)
  }

  NULL
}
