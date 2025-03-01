for (n in names(chinook_connections)) {
  conn <- chinook_connections[[n]]

  test_that(paste0("setcolorder works", " [", n, "]"), {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))
    expect_no_error(Artist <- dbi.table(conn, DBI::Id("Artist")))

    expect_true(reference.test({
      x <- merge(Album, Artist, by = "ArtistId")
      setcolorder(x, c(4, 2, 3, 1))
    }))

    expect_true(reference.test({
      setcolorder(Artist, 2:1)
    }))

    expect_true(reference.test({
      setcolorder(Album, c("AlbumId", "ArtistId", "Title"))
    }))
  })
}
