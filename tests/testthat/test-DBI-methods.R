chinook <- dbi.catalog(chinook.duckdb)

test_that("DBI::{Create,Append,Remove}Table work", {
  expect_no_error({
    DBI::dbCreateTable(chinook, "iris", iris)
  })

  expect_no_error({
    DBI::dbAppendTable(chinook, "iris", iris)
  })

  expect_no_error({
    DBI::dbRemoveTable(chinook, "iris")
  })

  expect_no_error({
    DBI::dbCreateTable(chinook$main, "iris", iris)
  })

  expect_no_error({
    DBI::dbAppendTable(chinook$main, "iris", iris)
  })

  expect_no_error({
    DBI::dbRemoveTable(chinook$main, "iris")
  })

  expect_no_error({
    DBI::dbCreateTable(chinook$main$Track, "iris", iris)
  })

  expect_no_error({
    DBI::dbAppendTable(chinook$main$Track, "iris", iris)
  })

  expect_no_error({
    DBI::dbRemoveTable(chinook$main$Track, "iris")
  })
})



test_that("DBI::{Write,Read}Table work", {
  expect_no_error({
    DBI::dbWriteTable(chinook, "iris", iris)
  })

  expect_identical(DBI::dbReadTable(chinook, "iris"), iris)

  expect_no_error({
    DBI::dbRemoveTable(chinook, "iris")
  })

    expect_no_error({
    DBI::dbWriteTable(chinook$main, "iris", iris)
  })

  expect_identical(DBI::dbReadTable(chinook$main, "iris"), iris)

  expect_no_error({
    DBI::dbRemoveTable(chinook$main, "iris")
  })

  expect_no_error({
    DBI::dbWriteTable(chinook$main$Track, "iris", iris)
  })

  expect_identical(DBI::dbReadTable(chinook$main$Track, "iris"), iris)

  expect_no_error({
    DBI::dbRemoveTable(chinook$main$Track, "iris")
  })
})



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



test_that("DBI::dbWithTransaction works", {
  expect_no_error(
    DBI::dbWriteTable(chinook, "cash", data.frame(amount = 100))
  )

  expect_no_error(
    DBI::dbWriteTable(chinook, "account", data.frame(amount = 2000))
  )

  expect_no_error(DBI::dbWithTransaction(chinook, {
    withdrawal <- 300
    DBI::dbExecute(chinook, "UPDATE cash SET amount = amount + ?",
                   list(withdrawal))
    DBI::dbExecute(chinook, "UPDATE account SET amount = amount - ?",
                   list(withdrawal))
  }))

  expect_no_error(DBI::dbWithTransaction(chinook$main, {
    withdrawal <- 300
    DBI::dbExecute(chinook$main, "UPDATE cash SET amount = amount + ?",
                   list(withdrawal))
    DBI::dbExecute(chinook$main, "UPDATE account SET amount = amount - ?",
                   list(withdrawal))
  }))

  expect_no_error(DBI::dbWithTransaction(chinook$main$Album, {
    withdrawal <- 300
    DBI::dbExecute(chinook$main$Album, "UPDATE cash SET amount = amount + ?",
                   list(withdrawal))
    DBI::dbExecute(chinook$main$Album, "UPDATE account SET amount = amount - ?",
                   list(withdrawal))
  }))

  expect_equal(DBI::dbReadTable(chinook, "cash")$amount, 1000)
  expect_equal(DBI::dbReadTable(chinook, "account")$amount, 1100)

  expect_no_error(DBI::dbRemoveTable(chinook, "cash"))
  expect_no_error(DBI::dbRemoveTable(chinook, "account"))
})
