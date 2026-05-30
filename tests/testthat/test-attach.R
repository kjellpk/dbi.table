test_that("dbi.attach works on SQLite", {
  expect_no_error(dbi.attach(chinook.sqlite, pos = 2L))
  expect_type(e <- as.environment(2L), type = "environment")
  expect_s3_class(e[["../catalog"]], "dbi.catalog")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))

  expect_silent(detach(2L))
})

test_that("dbi.attach works on DuckDB", {
  expect_no_error(dbi.attach(chinook.duckdb, pos = 2L))
  expect_type(e <- as.environment(2L), type = "environment")
  expect_s3_class(e[["../catalog"]], "dbi.catalog")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))

  expect_silent(detach(2L))
})
