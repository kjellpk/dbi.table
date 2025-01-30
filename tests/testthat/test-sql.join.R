conns <- list(chinook.sqlite(), chinook.duckdb())

for (conn in conns) {

  test_that("inner join works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "inner", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })



  test_that("left join works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "left", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })



  test_that("right join works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "right", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })



  test_that("outer join works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "outer", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })



  test_that("cross join works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "cross")
    )
    expect_true(is.dbi.table(x))
  })



  test_that("cross join throws warning when on != NULL", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_error(
      x <- sql.join(Album, Artist, "cross", on = x.ArtistId == 2*y.ArtistId)
    )
  })



  test_that("self join works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_no_error(
      x <- sql.join(Album, Album, on = x.ArtistId == y.AlbumId)
    )
    expect_true(is.dbi.table(x))
  })



  DBI::dbDisconnect(conn)
}