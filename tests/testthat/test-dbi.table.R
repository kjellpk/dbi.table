source("compare_with_data.table.R")



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
  expect_no_error(source("compare_with_data.table.R"))
  expect_true(compare_with_data.table(
    Album[AlbumId > 7 & AlbumId < 13]
  ))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})



test_that("window works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_no_error(source("compare_with_data.table.R"))
  expect_true(compare_with_data.table(
    Track[, .(TrackId,
              x = as.numeric(Milliseconds) / sum(as.numeric(Milliseconds)))]
  ))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})



test_that("aggregation works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_no_error(source("compare_with_data.table.R"))
  expect_true(compare_with_data.table(
    Track[, .(SUM = sum(Milliseconds),
              MEAN = mean(Milliseconds))]
  ))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})
