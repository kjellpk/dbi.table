for (n in names(chinook_connections)) {
  conn <- chinook_connections[[n]]

  test_that(paste0("dbi.table works", " [", n, "]"), {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))
    expect_true(is.dbi.table(Album))
  })

  test_that(paste0("simple subset works", " [", n, "]"), {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))
    expect_true(reference.test(
      Album[AlbumId > 7 & AlbumId < 13],
      verbose = FALSE
    ))
  })

  test_that(paste0("aggregation works", " [", n, "]"), {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(SUM = sum(Milliseconds, na.rm = TRUE),
                MEAN = mean(Milliseconds, na.rm = TRUE))],
      verbose = FALSE
    ))
  })

  test_that(paste0("aggregation works with by", " [", n, "]"), {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(SUM = sum(Milliseconds, na.rm = TRUE),
                MEAN = mean(Milliseconds, na.rm = TRUE)),
            by = .(GenreId)],
      verbose = FALSE
    ))
  })

  test_that(paste0("window works", " [", n, "]"), {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(TrackId,
                x = as.numeric(Milliseconds) /
                  sum(as.numeric(Milliseconds), na.rm = TRUE))],
      verbose = FALSE
    ))
  })

  test_that(paste0("window works with by", " [", n, "]"), {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(TrackId,
                x = as.numeric(Milliseconds) /
                  sum(as.numeric(Milliseconds), na.rm = TRUE)),
            by = .(GenreId)],
      verbose = FALSE
    ))
  })

  test_that(paste0("j syntax consistent with data.table", " [", n, "]"), {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))

    expect_true(reference.test(
      Track[, Name:MediaTypeId],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Track[, 1:MediaTypeId],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Track[, Name:6],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Track[, 1:6],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Track[, c("MediaTypeId", "UnitPrice", "Name", "Milliseconds")],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Track[, c(4, 2, 3, 1)],
      verbose = FALSE
    ))

    tmp <- c("MediaTypeId", "UnitPrice", "Name", "Milliseconds")

    expect_error(Track[][, tmp])
    expect_error(Track[, tmp])

    expect_true(reference.test(
      Track[, ..tmp],
      verbose = FALSE
    ))

    # expect_true(reference.test(
    #   Track[, c(..tmp)]
    # ))

    # expect_true(reference.test(
    #   Track[, c("Milliseconds", ..tmp[1:3])]
    # ))

    # dbi.table returns dbi.table; data.table returns numeric
    expect_no_error(Track[, .N])

    expect_true(reference.test(
      Track[, list(N = .N)],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Track[, .N, MediaTypeId],
      verbose = FALSE
    ))
  })

  test_that(paste0("local works", " [", n, "]"), {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))

    expect_true(reference.test(
      Album[ArtistId == ArtistId],
      verbose = FALSE
    ))

    expect_no_error(ArtistId <- 7)
    expect_true(Album[ArtistId == local(ArtistId), .N][]$N == 1L)
  })

  test_that(paste0("i a dbi.table, data.table, or data.frame works",
                   " [", n, "]"), {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))
    expect_no_error(id2 <- unique(Album[ArtistId == 2, .(ArtistId)]))

    expect_true(reference.test(
      Album[id2, on = "ArtistId"],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Album[data.table(ArtistId = 2), on = "ArtistId"],
      verbose = FALSE
    ))

    expect_true(reference.test(
      Album[data.frame(ArtistId = 2), on = "ArtistId"],
      verbose = FALSE
    ))
  })

  test_that(paste0("empty temporary tables work", " [", n, "]"), {
    expect_true(
      nrow(as.dbi.table(conn, datasets::iris[0, ], type = "query")[]) == 0L
    )

    expect_true(
      nrow(as.dbi.table(conn, datasets::iris[0, ], type = "temporary")[]) == 0L
    )
  })

  test_that(paste0("over picks up key", " [", n, "]"), {
    expect_no_error(
      Album <- dbi.table(conn, "Album", key = c("ArtistId", "Title"))
    )

    expect_true(reference.test(
      Album[, .(AlbumId, s = sum(AlbumId), cs = cumsum(AlbumId))],
      ignore.row.order = FALSE, verbose = FALSE
    ))
  })

  test_that(paste0("order works w/ key", " [", n, "]"), {
    expect_no_error(
      Album <- dbi.table(conn, "Album", key = c("Title"))
    )

    expect_true(reference.test(
      Album[order(AlbumId), .(AlbumId, cs = cumsum(AlbumId))],
      ignore.row.order = FALSE, verbose = FALSE
    ))
  })
}
