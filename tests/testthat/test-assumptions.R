test_that("dbplyr::translate_sql_ borks", {
  conn <- dbplyr::simulate_dbi()

  expect_no_error({
    dbplyr::translate_sql(title %like% "bork", con = conn)
  })

  expect_error({
    dbplyr::translate_sql_(list(quote(title %like% "bork")), con = conn)
  })
})
