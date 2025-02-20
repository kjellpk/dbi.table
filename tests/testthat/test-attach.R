test_that("dbi.attach works on SQLite", {
  expect_no_error(dbi.attach(chinook.sqlite, pos = 2L))
  expect_type(e <- as.environment(2L), type = "environment")
  expect_s3_class(e[["../catalog"]], "dbi.catalog")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))
  expect_warning(eval(quote(Album[, z := 42]), envir = e))
  expect_silent(detach(2L))
})

test_that("dbi.attach works on DuckDB", {
  expect_no_error(dbi.attach(chinook.duckdb, pos = 2L))
  expect_type(e <- as.environment(2L), type = "environment")
  expect_s3_class(e[["../catalog"]], "dbi.catalog")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))
  expect_warning(eval(quote(Album[, z := 42]), envir = e))
  expect_silent(detach(2L))
})

test_that("dbi.attach works on Postgres", {
  skip_on_cran()
  expect_no_error(e <- dbi.attach(rnacentral.postgres)) #works b/c only 1 schema
  expect_true(identical(as.environment(2L), e))
  expect_equal(length(ls(e[["../catalog"]])), 3L)
  expect_true("information_schema" %in% ls(e[["../catalog"]]))
  expect_true("rnacen" %in% ls(e[["../catalog"]]))
  expect_silent(detach(2L))
})

test_that("dbi.attach works on Postgres w/ schema arg", {
  skip_on_cran()
  expect_no_error(e <- dbi.attach(rnacentral.postgres, schema = "rnacen"))
  expect_true(identical(as.environment(2L), e))
  expect_equal(length(ls(e[["../catalog"]])), 2L)
  expect_true("information_schema" %in% ls(e[["../catalog"]]))
  expect_true("rnacen" %in% ls(e[["../catalog"]]))
  expect_silent(detach(2L))
})

test_that("dbi.attach works on MariaDB / needs schema arg", {
  skip_on_cran()
  expect_error(e <- dbi.attach(ctu.mariadb)) #since not interactive / multiple schemas
  expect_no_error(e <- dbi.attach(ctu.mariadb, schema = "Chinook"))
  expect_true(identical(as.environment(2L), e))
  expect_equal(length(ls(e[["../catalog"]])), 2L)
  expect_true("information_schema" %in% ls(e[["../catalog"]]))
  expect_true("Chinook" %in% ls(e[["../catalog"]]))
  expect_silent(detach(2L))
})
