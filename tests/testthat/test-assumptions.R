test_that("dbplyr::translate_sql_ borks", {
  conn <- dbplyr::simulate_dbi()

  expect_no_error({
    dbplyr::translate_sql(title %like% "bork", con = conn)
  })

  expect_error({
    dbplyr::translate_sql_(list(quote(title %like% "bork")), con = conn)
  })
})



test_that("RMariaDB includes def when dbQuoteIdentifiering Ids", {
  conn <- dbplyr::simulate_mariadb()
  name <- c(catalog = "def", schema = "apples", table = "honeycrisp")
  id <- DBI::Id(name)

  expect_identical(
    RMariaDB:::dbQuoteIdentifier_MariaDBConnection_Id(conn, id),
    DBI::SQL(paste(DBI::dbQuoteIdentifier(conn, name), collapse = "."))
  )
})
