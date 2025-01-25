conns <- list(chinook.sqlite(), chinook.duckdb())

for (conn in conns) {

  test_that("inner merge works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId"),
      verbose = FALSE
    ))
  })



  test_that("left merge works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
            all.x = TRUE)[is.na(Name)],
      verbose = FALSE
    ))
  })



  test_that("right merge works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Genre, Album, by.x = "GenreId", by.y = "AlbumId",
            all.y = TRUE)[is.na(Name)],
      verbose = FALSE
    ))
  })



  test_that("outer merge works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Genre <- dbi.table(conn, DBI::Id("Genre"))
    expect_true(reference.test(
      merge(Album, Genre, by.x = "AlbumId", by.y = "GenreId",
            all = TRUE)[is.na(Name) | is.na(Title)],
      verbose = FALSE
    ))
  })



  test_that("by works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_true(reference.test(
      merge(Album, Artist, by = "ArtistId"),
      verbose = FALSE
    ))
  })



  test_that("sometimes merge doesn't work", {
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



  test_that("self merge works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      merge(Album, Album, by = c("AlbumId", "ArtistId")),
      verbose = FALSE
    ))
  })



  test_that("self merge works with no.dups = FALSE", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_warning(reference.test(
      merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId", no.dups = FALSE),
      verbose = FALSE
    ))
    expect_true(reference.test(
      suppressWarnings(
        merge(Album, Album, by.x = "AlbumId", by.y = "ArtistId",
              no.dups = FALSE)
      ),
      verbose = FALSE
    ))
  })



  test_that("extract merge works", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_true(reference.test(
      Album[Artist, on = "ArtistId"],
      verbose = FALSE
    ))
  })



  test_that("extract merge foreign key join", {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer, on = c(TrackId = "CustomerId", AlbumId = "SupportRepId")],
      verbose = FALSE
    ))
  })



  test_that("extract merge w/ foreign key joins using the binary operator ==", {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer,
            on = c("TrackId == CustomerId",
                   "AlbumId == SupportRepId")],
      verbose = FALSE
    ))
  })



  test_that("extract merge w/ syntax as X[Y, on=.(a, b)]", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    expect_true(reference.test(
      Album[Artist, on = .(AlbumId == ArtistId, ArtistId == ArtistId)],
      verbose = FALSE
    ))
  })



  test_that("extract merge w/ (non-equi) joins using binary operators >=, <=", {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer,
            on = c("TrackId >= CustomerId",
                   "AlbumId <= SupportRepId")],
      verbose = FALSE
    ))
  })



  test_that("extract merge w/ (non-equi) joins using binary operators >, <", {
    Track <- dbi.table(conn, DBI::Id("Track"))
    Customer <- dbi.table(conn, DBI::Id("Customer"))
    expect_true(reference.test(
      Track[Customer, on = c("TrackId > CustomerId", "AlbumId < SupportRepId")],
      verbose = FALSE
    ))
  })



  test_that("extract self-merge withs w/ half-named character", {
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Album[Album, on = c("AlbumId", ArtistId = "ArtistId")],
      verbose = FALSE
    ))
  })



  test_that("extract anti-join works w/ character", {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = "ArtistId"],
      verbose = FALSE
    ))
  })



  test_that("extract anti-join works w/ character non-equi join", {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = "ArtistId > ArtistId"],
      verbose = FALSE
    ))
  })



  test_that("extract anti-join works w/ call non-equi join", {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = .(ArtistId > ArtistId)],
      verbose = FALSE
    ))
  })



  test_that("extract anti-join works w/ char-call non-equi join", {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))
    expect_true(reference.test(
      Artist[!Album, on = c("ArtistId > ArtistId")],
      verbose = FALSE
    ))
  })



  test_that("merge preserves where and order by", {
    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))

    expect_true(reference.test({
      Artist <- Artist[ArtistId > 5 & ArtistId < 10]
      Album <- Album[nchar(Title) > 15]
      merge(Album, Artist, by = "ArtistId")},
      verbose = FALSE
    ))

    Artist <- dbi.table(conn, DBI::Id("Artist"))
    Album <- dbi.table(conn, DBI::Id("Album"))

    expect_true(reference.test({
      Artist <- Artist[order(Name)]
      Album <- Album[order(-Title)]
      merge(Album, Artist, by = "ArtistId")},
      verbose = FALSE
    ))
  })

  DBI::dbDisconnect(conn)
}
