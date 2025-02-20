for (n in names(chinook_connections)) {
  conn <- chinook_connections[[n]]

  test_that(paste0("inner join works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "inner", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })

  test_that(paste0("left join works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "left", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })

  test_that(paste0("right join works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "right", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })

  test_that(paste0("outer join works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "outer", on = x.ArtistId == 2*y.ArtistId)
    )
    expect_true(is.dbi.table(x))
  })

  test_that(paste0("cross join works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_no_error(
      x <- sql.join(Album, Artist, "cross")
    )
    expect_true(is.dbi.table(x))
  })

  test_that(paste0("cross join throws warning when on != NULL", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_error(
      x <- sql.join(Album, Artist, "cross", on = x.ArtistId == 2*y.ArtistId)
    )
  })

  test_that(paste0("self join works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_no_error(
      x <- sql.join(Album, Album, on = x.ArtistId == y.AlbumId)
    )
    expect_true(is.dbi.table(x))
  })
}
