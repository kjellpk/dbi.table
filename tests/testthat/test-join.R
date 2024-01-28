test_that("inner join works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(
    x <- join(Album, Artist, "inner", on = x.ArtistId == 2*y.ArtistId)
  )
  expect_true(is.dbi.table(x))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("left join works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(
    x <- join(Album, Artist, "left", on = x.ArtistId == 2*y.ArtistId)
  )
  expect_true(is.dbi.table(x))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("right join works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(
    x <- join(Album, Artist, "right", on = x.ArtistId == 2*y.ArtistId)
  )
  expect_true(is.dbi.table(x))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("outer join works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(
    x <- join(Album, Artist, "outer", on = x.ArtistId == 2*y.ArtistId)
  )
  expect_true(is.dbi.table(x))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("cross join works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_no_error(
    x <- join(Album, Artist, "cross")
  )
  expect_true(is.dbi.table(x))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("cross join throws warning when on != NULL", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(Artist <- dbi.table(conn, DBI::Id(table = "Artist")))
  expect_warning(
    x <- join(Album, Artist, "cross", on = x.ArtistId == 2*y.ArtistId)
  )
  expect_true(is.dbi.table(x))
  expect_no_error(DBI::dbDisconnect(conn))
})



test_that("self join works", {
  expect_no_error(conn <- chinook.sqlite())
  expect_no_error(Album <- dbi.table(conn, DBI::Id(table = "Album")))
  expect_no_error(
    x <- join(Album, Album, on = x.ArtistId == y.AlbumId)
  )
  expect_true(is.dbi.table(x))
  expect_no_error(DBI::dbDisconnect(conn))
})
