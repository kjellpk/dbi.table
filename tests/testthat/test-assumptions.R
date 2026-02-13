test_that("copy_vector actually copies vector", {
  v1 <- "vector"
  a1 <- data.table::address(v1)
  v2 <- copy_vector(v1)
  a2 <- data.table::address(v2)
  expect_identical(v1, v2)
  expect_true(!identical(a1, a2))
})
