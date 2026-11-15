for (n in names(chinook_connections)) {
  conn <- chinook_connections[[n]]

  test_that(paste0("key is renamed/preserved", " [", n, "]"), {
    Track <- dbi.table(conn, DBI::Id("Track"), key = "TrackId")

    expect_true(reference.test({
      Track[, .(Bob = TrackId, Name, TrackId)]
    }))
  })
}
