for (n in names(chinook_connections)) {
  conn <- chinook_connections[[n]]

  test_that(paste0("inner merge works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId")
    ))
  })

  test_that(paste0("left merge works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
            all.x = TRUE)[is.na(Name)]
    ))
  })

  test_that(paste0("right merge works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Genre, Album, by.x = "GenreId", by.y = "AlbumId",
            all.y = TRUE)[is.na(Name)]
    ))
  })

  test_that(paste0("outer merge works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
            all = TRUE)[is.na(Name) | is.na(Title)]
    ))
  })

  test_that(paste0("by works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_true(reference.test(
      merge(Album, Artist, by = "ArtistId")
    ))
  })

  test_that(paste0("sometimes merge doesn't work", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_error(merge(Album, Genre))
    expect_error(merge(as.data.table(Album), as.data.table(Genre)))
    expect_error(merge(Album, Genre, by.x = "AlbumId"))
    expect_error(merge(as.data.table(Album), as.data.table(Genre),
                       by.x = "AlbumId"))
    expect_error(merge(Album, Genre, by.x = character(0), by.y = character(0)))
    expect_error(merge(as.data.table(Album), as.data.table(Genre),
                       by.x = character(0), by.y = character(0)))
  })

  test_that(paste0("self merge works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      merge(Album, Album, by = c("AlbumId", "ArtistId"))
    ))
  })

  test_that(paste0("self merge works with no.dups = FALSE", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_warning(reference.test(
      merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId", no.dups = FALSE)
    ))
    expect_true(reference.test(
      suppressWarnings(
        merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId",
              no.dups = FALSE)
      )
    ))
  })

  test_that(paste0("extract merge works", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_true(reference.test(
      Album[Artist, on = "ArtistId"]
    ))
  })

  test_that(paste0("extract merge foreign key join", " [", n, "]"), {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer, on = c(TrackId = "CustomerId", AlbumId = "SupportRepId")]
    ))
  })

  test_that(paste0("extract merge w/ foreign key joins using the binary operator ==", " [", n, "]"), {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer,
            on = c("TrackId == CustomerId",
                   "AlbumId == SupportRepId")]
    ))
  })

  test_that(paste0("extract merge w/ syntax as X[Y, on=.(a, b)]", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_true(reference.test(
      Album[Artist, on = .(AlbumId == ArtistId, ArtistId == ArtistId)]
    ))
  })

  test_that(paste0("extract merge w/ syntax as X[Y, on=.(x1 = a, x2 = b)]", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_true(reference.test(
      Album[Artist, on = .(AlbumId = ArtistId, ArtistId = ArtistId)]
    ))
  })

  test_that(paste0("extract merge w/ (non-equi) joins using binary operators >=, <=", " [", n, "]"), {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer,
            on = c("TrackId >= CustomerId",
                   "AlbumId <= SupportRepId")]
    ))
  })

  test_that(paste0("extract merge w/ (non-equi) joins using binary operators >, <", " [", n, "]"), {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer, on = c("TrackId > CustomerId", "AlbumId < SupportRepId")]
    ))
  })

  test_that(paste0("extract self-merge withs w/ half-named character", " [", n, "]"), {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Album[Album, on = c("AlbumId", ArtistId = "ArtistId")]
    ))
  })

  test_that(paste0("extract anti-join works w/ character", " [", n, "]"), {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = "ArtistId"]
    ))
  })

  test_that(paste0("extract anti-join works w/ character non-equi join", " [", n, "]"), {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = "ArtistId > ArtistId"]
    ))
  })

  test_that(paste0("extract anti-join works w/ call non-equi join", " [", n, "]"), {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = .(ArtistId > ArtistId)]
    ))
  })

  test_that(paste0("extract anti-join works w/ char-call non-equi join", " [", n, "]"), {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = c("ArtistId > ArtistId")]
    ))
  })

  test_that(paste0("merge preserves where and order by", " [", n, "]"), {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))

    expect_true(reference.test({
      Artist <- Artist[ArtistId > 5 & ArtistId < 10]
      Album <- Album[nchar(Title) > 15]
      merge(Album, Artist, by = "ArtistId")}
    ))

    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))

    expect_true(reference.test({
      Artist <- Artist[order(Name)]
      Album <- Album[order(-Title)]
      merge(Album, Artist, by = "ArtistId")}
    ))
  })

  test_that(paste0("merge default columns match data.table", " [", n, "]"), {

    # When x and y have a shared key column (ArtistId), it is the used as the
    # default column to merge on.

    x <- dbi.table(conn, DBI::Id("Album"), key = "ArtistId")
    y <- dbi.table(conn, DBI::Id("Genre"), key = "GenreId")
    names(y) <- c("ArtistId", "Title")

    for (allx in c(FALSE, TRUE)) {
      for (ally in c(FALSE, TRUE)) {
        expect_true(reference.test({
          merge(x, y, all.x = allx, all.y = ally)
        }))
      }
    }

    # When x has a key and y does not, then the default columns to merge on
    # are the shared columns of key(x) and y.

    x <- dbi.table(conn, DBI::Id("Album"), key = "ArtistId")
    y <- dbi.table(conn, DBI::Id("Genre"))
    names(y) <- c("ArtistId", "Title")

    for (allx in c(FALSE, TRUE)) {
      for (ally in c(FALSE, TRUE)) {
        expect_true(reference.test({
          merge(x, y, all.x = allx, all.y = ally)
        }))
      }
    }

    # When neither x nor y have a key, then the default columns to merge on
    # are the shared columns of x and y.

    x <- dbi.table(conn, DBI::Id("Album"))
    y <- dbi.table(conn, DBI::Id("Genre"))
    names(y) <- c("ArtistId", "Name")

    for (allx in c(FALSE, TRUE)) {
      for (ally in c(FALSE, TRUE)) {
        expect_true(reference.test({
          merge(x, y, all.x = allx, all.y = ally)
        }))
      }
    }

  })
}
