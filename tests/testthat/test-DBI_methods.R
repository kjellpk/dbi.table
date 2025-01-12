test_that("dbplyr::translate_sql_ borks", {
  chinook <- dbi.catalog(chinook.duckdb)

  expect_no_error({
    DBI::dbExecute(chinook, DBI::SQL("LOAD parquet;"))
  })

  expect_no_error({
    DBI::dbExecute(chinook$main, DBI::SQL("LOAD parquet;"))
  })

  expect_no_error({
    DBI::dbExecute(chinook$main$Album, DBI::SQL("LOAD parquet;"))
  })

  expect_vector({
    DBI::dbGetInfo(chinook)
  }, ptype = list(), size = 5L)

  expect_vector({
    DBI::dbGetInfo(chinook$main)
  }, ptype = list(), size = 5L)

  expect_vector({
    DBI::dbGetInfo(chinook$main$InvoiceLine)
  }, ptype = list(), size = 5L)

  expect_s4_class({
    res <- DBI::dbSendStatement(chinook$main$Album, n = 9L)
  }, class = "duckdb_result")

  expect_s3_class({
    x <- DBI::dbFetch(res, n = 8L)
  }, class = "data.frame")

  expect_equal(nrow(x), 8L)

  expect_s3_class({
    x <- DBI::dbFetch(res)
  }, class = "data.frame")

  expect_equal(nrow(x), 1L)

  expect_s3_class({
    x <- DBI::dbFetch(res)
  }, class = "data.frame")

  expect_equal(nrow(x), 0L)

  expect_no_error({
    DBI::dbClearResult(res)
  })

  expect_s3_class({
    x <- DBI::dbGetQuery(chinook$main$Track)
  }, class = "data.frame")

  expect_equal(dim(x), c(3503L, 9L))

  expect_s3_class({
    x <- DBI::dbGetQuery(chinook$main$Track, n = 7L)
  }, class = "data.frame")

  expect_equal(dim(x), c(7L, 9L))
})
