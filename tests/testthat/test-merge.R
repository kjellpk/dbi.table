test_that("inner merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId"),
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("left merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
          all.x = TRUE)[is.na(Name)],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("right merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Genre, Album, by.x = "GenreId", by.y = "AlbumId",
          all.y = TRUE)[is.na(Name)],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("outer merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Genre <- dbi.table(conn, DBI::Id("Genre")))
  expect_true(reference_test(
    merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
          all = TRUE)[is.na(Name) | is.na(Title)],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("by works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id("Artist")))
  expect_true(reference_test(
    merge(Album, Artist, by = "ArtistId"),
    verbose = FALSE
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
    merge(Album, Album, by = c("AlbumId", "ArtistId")),
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("self merge works with no.dups = FALSE", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_warning(reference_test(
    merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId", no.dups = FALSE),
    verbose = FALSE
  ))
  expect_true(reference_test(
    suppressWarnings(
      merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId", no.dups = FALSE)
    ),
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket merge works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_true(reference_test(
    Album[Artist, on = "ArtistId"],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket merge foreign key join", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_no_error(Customer <- dbi.table(conn, DBI::Id(table = "Customer")))
  expect_true(reference_test(
    Track[Customer, on = c(TrackId = "CustomerId", AlbumId = "SupportRepId")],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket merge w/ foreign key joins using the binary operator ==", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_no_error(Customer <- dbi.table(conn, DBI::Id(table = "Customer")))
  expect_true(reference_test(
    Track[Customer, on = c("TrackId == CustomerId", "AlbumId == SupportRepId")],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket merge w/ syntax as X[Y, on=.(a, b)]", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_true(reference_test(
    Album[Artist, on = .(AlbumId == ArtistId, ArtistId == ArtistId)],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket merge w/ (non-equi) joins using binary operators >=, <=", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_no_error(Customer <- dbi.table(conn, DBI::Id(table = "Customer")))
  expect_true(reference_test(
    Track[Customer, on = c("TrackId >= CustomerId", "AlbumId <= SupportRepId")],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket merge w/ (non-equi) joins using binary operators >, <", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Track <- dbi.table(conn, DBI::Id(table = "Track")))
  expect_no_error(Customer <- dbi.table(conn, DBI::Id(table = "Customer")))
  expect_true(reference_test(
    Track[Customer, on = c("TrackId > CustomerId", "AlbumId < SupportRepId")],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket self-merge withs w/ half-named character", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    Album[Album, on = c("AlbumId", ArtistId = "ArtistId")],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket anti-join works w/ character", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    Artist[!Album, on = "ArtistId"],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket anti-join works w/ character non-equi join", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    Artist[!Album, on = "ArtistId > ArtistId"],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("bracket anti-join works w/ call non-equi join", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    Artist[!Album, on = .(ArtistId > ArtistId)],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})




test_that("bracket anti-join works w/ char-call non-equi join", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_true(reference_test(
    Artist[!Album, on = c("ArtistId > ArtistId")],
    verbose = FALSE
  ))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("merge preserves where and order by", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))

  expect_true(reference_test({
    Artist <- Artist[ArtistId > 5 & ArtistId < 10]
    Album <- Album[nchar(Title) > 15]
    merge(Album, Artist, by = "ArtistId")},
    verbose = FALSE
  ))

  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))

  expect_true(reference_test({
    Artist <- Artist[order(Name)]
    Album <- Album[order(-Title)]
    merge(Album, Artist, by = "ArtistId")},
    verbose = FALSE
  ))

  expect_no_error(DBI::dbDisconnect(conn))
})
