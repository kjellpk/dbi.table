test_that("inner merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId")
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("left merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
          all.x = TRUE)[is.na(Name)]
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("right merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
          all.y = TRUE)[is.na(Title)]
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("outer merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
          all = TRUE)[is.na(Name) | is.na(Title)]
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("outer merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
          all = TRUE)[is.na(Name) | is.na(Title)]
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("no.dups works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId")
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("self merge works with no.dups = FALSE", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId", no.dups = FALSE)
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})
