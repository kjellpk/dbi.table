test_that("dbi.table works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(is.dbi.table(Album))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("simple subset works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    Album[AlbumId > 7 & AlbumId < 13],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("aggregation works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_true(reference_test(
    Track[, .(SUM = sum(Milliseconds, na.rm = TRUE),
              MEAN = mean(Milliseconds, na.rm = TRUE))],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("aggregation works with by", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_true(reference_test(
    Track[, .(SUM = sum(Milliseconds, na.rm = TRUE),
              MEAN = mean(Milliseconds, na.rm = TRUE)),
          by = .(GenreId)],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("window works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_true(reference_test(
    Track[, .(TrackId,
              x = as.numeric(Milliseconds) /
                sum(as.numeric(Milliseconds), na.rm = TRUE))],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("window works with by", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_true(reference_test(
    Track[, .(TrackId,
              x = as.numeric(Milliseconds) /
                sum(as.numeric(Milliseconds), na.rm = TRUE)),
          by = .(GenreId)],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("j syntax consistent with data.table", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))

  expect_true(reference_test(
    Track[, Name:MediaTypeId],
    verbose = FALSE
  ))

  expect_true(reference_test(
    Track[, 1:MediaTypeId],
    verbose = FALSE
  ))

  expect_true(reference_test(
    Track[, Name:6],
    verbose = FALSE
  ))

  expect_true(reference_test(
    Track[, 1:6],
    verbose = FALSE
  ))

  expect_true(reference_test(
    Track[, c("MediaTypeId", "UnitPrice", "Name", "Milliseconds")],
    verbose = FALSE
  ))

  expect_true(reference_test(
    Track[, c(4, 2, 3, 1)],
    verbose = FALSE
  ))

  expect_no_error(tmp <- c("MediaTypeId", "UnitPrice", "Name", "Milliseconds"))

  expect_error(Track[][, tmp])
  expect_error(Track[, tmp]) # currently for wrong reason

  # expect_true(reference_test(
  #   Track[, ..tmp]
  # ))

  # expect_true(reference_test(
  #   Track[, c(..tmp)]
  # ))

  # expect_true(reference_test(
  #   Track[, c("Milliseconds", ..tmp[1:3])]
  # ))

  expect_no_error(DBI::dbDisconnect(conn))
})
