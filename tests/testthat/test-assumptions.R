test_that("dbplyr::translate_sql_ borks", {
  conn <- dbplyr::simulate_dbi()

  expect_no_error({
    dbplyr::translate_sql(title %like% "bork", con = conn)
  })

  expect_error({
    dbplyr::translate_sql_(list(quote(title %like% "bork")), con = conn)
  })
})


test_that("copy_vector actually copies vector", {
  v1 <- "vector"
  a1 <- data.table::address(v1)
  v2 <- copy_vector(v1)
  a2 <- data.table::address(v2)
  expect_identical(v1, v2)
  expect_true(!identical(a1, a2))
})
