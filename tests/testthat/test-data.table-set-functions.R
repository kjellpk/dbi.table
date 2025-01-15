test_that("set* functions work", {
  conns <- list(chinook.sqlite(), chinook.duckdb())

  for (conn in conns) {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))
    expect_no_error(Artist <- dbi.table(conn, DBI::Id("Artist")))

    expect_true(reference.test({
      x <- merge(Album, Artist, by = "ArtistId")
      setcolorder(x, c(4, 2, 3, 1))
    }, verbose = FALSE))

    expect_true(reference.test({
      setcolorder(Artist, 2:1)
    }, verbose = FALSE))

    expect_true(reference.test({
      setcolorder(Album, c("AlbumId", "ArtistId", "Title"))
    }, verbose = FALSE))
  }

  lapply(conns, DBI::dbDisconnect)
})
