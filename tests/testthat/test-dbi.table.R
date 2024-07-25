conns <- list(chinook.sqlite(), chinook.duckdb())

for (conn in conns) {
  test_that("dbi.table works", {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))
    expect_true(is.dbi.table(Album))
  })



  test_that("simple subset works", {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))
    expect_true(reference.test(
      Album[AlbumId > 7 & AlbumId < 13],
      verbose = FALSE
    ))
  })



  test_that("aggregation works", {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(SUM = sum(Milliseconds, na.rm = TRUE),
                MEAN = mean(Milliseconds, na.rm = TRUE))],
      verbose = FALSE
    ))
  })



  test_that("aggregation works with by", {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(SUM = sum(Milliseconds, na.rm = TRUE),
                MEAN = mean(Milliseconds, na.rm = TRUE)),
            by = .(GenreId)],
      verbose = FALSE
    ))
  })



  test_that("window works", {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(TrackId,
                x = as.numeric(Milliseconds) /
                  sum(as.numeric(Milliseconds), na.rm = TRUE))],
      verbose = FALSE
    ))
  })



  test_that("window works with by", {
    expect_no_error(Track <- dbi.table(conn, DBI::Id("Track")))
    expect_true(reference.test(
      Track[, .(TrackId,
                x = as.numeric(Milliseconds) /
                  sum(as.numeric(Milliseconds), na.rm = TRUE)),
            by = .(GenreId)],
      verbose = FALSE
    ))
  })



  test_that("j syntax consistent with data.table", {
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

    expect_no_error(tmp <- c("MediaTypeId", "UnitPrice", "Name", "Milliseconds"))

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



  test_that("local works", {
    expect_no_error(Album <- dbi.table(conn, DBI::Id("Album")))

    expect_true(reference.test(
      Album[ArtistId == ArtistId],
      verbose = FALSE
    ))

    expect_no_error(ArtistId <- 7)
    expect_true(Album[ArtistId == local(ArtistId), .N][]$N == 1L)
  })

  DBI::dbDisconnect(conn)
}
