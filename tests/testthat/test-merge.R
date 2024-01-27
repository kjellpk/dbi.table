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
    merge(Genre, Album, by.x = "GenreId", by.y = "AlbumId",
          all.y = TRUE)[is.na(Name)]
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



test_that("by works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id("Artist")))
  expect_true(reference_test(
    merge(Album, Artist, by = "ArtistId")
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("sometimes merge doesn't work", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_error(merge(Album, Genre))
  expect_error(merge(as.data.table(Album), as.data.table(Genre)))
  expect_error(merge(Album, Genre, by.x = "AlbumId"))
  expect_error(merge(as.data.table(Album), as.data.table(Genre),
                     by.x = "AlbumId"))
  expect_error(merge(Album, Genre, by.x = character(0), by.y = character(0)))
  expect_error(merge(as.data.table(Album), as.data.table(Genre),
                     by.x = character(0), by.y = character(0)))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("self merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    merge(Album, Album, by = c("AlbumId", "ArtistId"))
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("self merge works with no.dups = FALSE", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_warning(reference_test(
    merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId", no.dups = FALSE)
  ))
  expect_true(reference_test(
    suppressWarnings(
      merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId", no.dups = FALSE)
    )
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})
