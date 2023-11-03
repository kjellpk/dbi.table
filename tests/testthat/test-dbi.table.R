source("compare_with_data.table.R")



test_that("dbi.table works", {
  expect_no_error(conn <- chinook.sqlite(type = "DBI"))
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(is.dbi.table(Album))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("dbi.attach works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(dbi.attach(conn))
  expect_type(e <- as.environment("RSQLite:Chinook_Sqlite"),
              type =  "environment")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))
  expect_silent(pool::poolClose(conn))
})



test_that("simple subset works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(dbi.attach(conn))
  expect_no_error(source("compare_with_data.table.R"))
  expect_true(compare_with_data.table(
    Album[AlbumId > 7 & AlbumId < 13],
    env = as.environment("RSQLite:Chinook_Sqlite")
  ))
  expect_silent(pool::poolClose(conn))
})



test_that("window works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(dbi.attach(conn))
  expect_no_error(source("compare_with_data.table.R"))
  expect_true(compare_with_data.table(
    Track[, .(TrackId,
              x = as.numeric(Milliseconds) / sum(as.numeric(Milliseconds)))],
    env = as.environment("RSQLite:Chinook_Sqlite")
  ))
  expect_true(compare_with_data.table(
    Track[, .(TrackId,
              x = as.numeric(Milliseconds) / sum(as.numeric(Milliseconds))),
          by = .(MediaTypeId)],
    env = as.environment("RSQLite:Chinook_Sqlite")
  ))
  expect_silent(pool::poolClose(conn))
})



test_that("aggregation works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(dbi.attach(conn))
  expect_no_error(source("compare_with_data.table.R"))
  expect_true(compare_with_data.table(
    Track[, .(SUM = sum(Milliseconds),
              MEAN = mean(Milliseconds))],
    env = as.environment("RSQLite:Chinook_Sqlite")
  ))
  expect_true(compare_with_data.table(
    Track[, .(SUM = sum(Milliseconds),
              MEAN = mean(Milliseconds)),
          by = .(MediaTypeId, GenreId)],
    env = as.environment("RSQLite:Chinook_Sqlite")
  ))
  expect_silent(pool::poolClose(conn))
})
