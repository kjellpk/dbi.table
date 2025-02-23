test_that("dbi.attach works on SQLite", {
  expect_no_error(dbi.attach(chinook.sqlite, pos = 2L))
  expect_type(e <- as.environment(2L), type = "environment")
  expect_s3_class(e[["../catalog"]], "dbi.catalog")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))

  # Using the walrus on a dbi.table in an attached schema should assign the
  # result in the calling frame (typically .GlobalEnv). It should not modify
  # the dbi.table in the attached schema.

  test_fun <- function() {
    Genre[, z := 42]
    names(Genre)
  }

  expect_identical(test_fun(), c(names(Genre), "z"))

  expect_true("Genre" %notin% ls())
  expect_s3_class(Genre[, z := 42], "dbi.table")
  expect_true("Genre" %in% ls())

  expect_silent(detach(2L))
})

test_that("dbi.attach works on DuckDB", {
  expect_no_error(dbi.attach(chinook.duckdb, pos = 2L))
  expect_type(e <- as.environment(2L), type = "environment")
  expect_s3_class(e[["../catalog"]], "dbi.catalog")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))

  # Using the walrus on a dbi.table in an attached schema should assign the
  # result in the calling frame (typically .GlobalEnv). It should not modify
  # the dbi.table in the attached schema.

  test_fun <- function() {
    Genre[, z := 42]
    names(Genre)
  }

  expect_identical(test_fun(), c(names(Genre), "z"))

  expect_true("Genre" %notin% ls())
  expect_s3_class(Genre[, z := 42], "dbi.table")
  expect_true("Genre" %in% ls())

  expect_silent(detach(2L))
})
