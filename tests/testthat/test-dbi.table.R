test_that("dbi.table works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(is.dbi.table(Album))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("dbi.attach works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_type(e <- as.environment("RSQLite:Chinook_Sqlite"),
              type =  "environment")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})



test_that("simple subset works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_true(reference_test(
    Album[AlbumId > 7 & AlbumId < 13],
    verbose = FALSE
  ))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})



test_that("window works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_true(reference_test(
    Track[, .(TrackId,
              x = as.numeric(Milliseconds) / sum(as.numeric(Milliseconds)))],
    verbose = FALSE
  ))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})



test_that("aggregation works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_true(reference_test(
    Track[, .(SUM = sum(Milliseconds),
              MEAN = mean(Milliseconds))],
    verbose = FALSE
  ))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})



test_that("j syntax consistent with data.table", {
  expect_no_error(dbi.attach(chinook.sqlite))

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

  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})
