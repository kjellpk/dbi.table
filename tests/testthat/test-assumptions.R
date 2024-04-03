test_that("dbplyr::translate_sql_ borks", {
  expect_no_error({
    conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  })

  expect_no_error({
    dbplyr::translate_sql(title %like% "bork", con = conn)
  })

  expect_error({
    dbplyr::translate_sql_(list(quote(title %like% "bork")), con = conn)
  })

  expect_no_error({
    DBI::dbDisconnect(conn)
  })
})
