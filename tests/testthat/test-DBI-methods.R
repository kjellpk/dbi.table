chinook <- dbi.catalog(chinook_connections$chinook_duckdb)

test_that("DBI::dbExecute works", {
  expect_no_error({
    DBI::dbExecute(chinook, DBI::SQL("LOAD parquet;"))
  })

  expect_no_error({
    DBI::dbExecute(chinook$main, DBI::SQL("LOAD parquet;"))
  })

  expect_no_error({
    DBI::dbExecute(chinook$main$Album, DBI::SQL("LOAD parquet;"))
  })
})



test_that("DBI::dbGetInfo works", {
  expect_vector({
    DBI::dbGetInfo(chinook)
  }, ptype = list(), size = 5L)

  expect_vector({
    DBI::dbGetInfo(chinook$main)
  }, ptype = list(), size = 5L)

  expect_vector({
    DBI::dbGetInfo(chinook$main$InvoiceLine)
  }, ptype = list(), size = 5L)
})



test_that("DBI::dbGetQuery works w/ statement missing", {
  expect_s3_class({
    x <- DBI::dbGetQuery(chinook$main$Track)
  }, class = "data.frame")

  expect_equal(dim(x), c(3503L, 9L))

  expect_s3_class({
    x <- DBI::dbGetQuery(chinook$main$Track, n = 7L)
  }, class = "data.frame")

  expect_equal(dim(x), c(7L, 9L))
})



test_that("DBI::dbGetQuery works w/ statement not missing", {
  expect_s3_class({
    x <- DBI::dbGetQuery(chinook, "SELECT * FROM Genre")
  }, class = "data.frame")

  expect_equal(dim(x), c(25L, 2L))

  expect_s3_class({
    x <- DBI::dbGetQuery(chinook$main, "SELECT * FROM Genre")
  }, class = "data.frame")

  expect_equal(dim(x), c(25L, 2L))

  expect_s3_class({
    x <- DBI::dbGetQuery(chinook$main$Track, "SELECT * FROM Genre")
  }, class = "data.frame")

  expect_equal(dim(x), c(25L, 2L))
})



test_that("DBI::dbSendStatement works w/ statement missing", {
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
})



test_that("DBI::dbSendStatement works w/ statement not missing", {
  expect_s4_class({
    res <- DBI::dbSendStatement(chinook, "SELECT * FROM Genre")
  }, class = "duckdb_result")

  expect_s3_class({
    x <- DBI::dbFetch(res)
  }, class = "data.frame")

  expect_no_error({
    DBI::dbClearResult(res)
  })

  expect_s4_class({
    res <- DBI::dbSendStatement(chinook$main, "SELECT * FROM Genre")
  }, class = "duckdb_result")

  expect_s3_class({
    x <- DBI::dbFetch(res)
  }, class = "data.frame")

  expect_no_error({
    DBI::dbClearResult(res)
  })

  expect_s4_class({
    res <- DBI::dbSendStatement(chinook$main$Album, "SELECT * FROM Genre")
  }, class = "duckdb_result")

  expect_s3_class({
    x <- DBI::dbFetch(res)
  }, class = "data.frame")

  expect_no_error({
    DBI::dbClearResult(res)
  })
})
