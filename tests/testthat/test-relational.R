# When relational information is not avaiable then dbi.table behaves like
# data.table.

test_that("merge when relational data not available", {
  conn <- chinook_connections$chinook_sqlite

  Album <- dbi.table(conn, DBI::Id("Album"))
  Genre <- dbi.table(conn, DBI::Id("Genre"))

  # a non-empty vector of column names for 'by' is required
  expect_error(merge(Album, Genre))

  # A non-empty vector of column names for `by` is required.
  expect_error(merge(as.data.table(Album), as.data.table(Genre)))

  # Merge on shared key column.
  Genre <- dbi.table(conn, DBI::Id("Genre"), key = "GenreId")
  names(Genre) <- c("ArtistId", "Name")
  Album <- dbi.table(conn, DBI::Id("Album"), key = "ArtistId")
  expect_true(reference.test(merge(Album, Genre)))

  # When there is no shared key column, then by = key(x).
  Album <- dbi.table(conn, DBI::Id("Album"), key = "ArtistId")
  Album2 <- dbi.table(conn, DBI::Id("Album"))
  names(Album2) <- c("AlbumId", "Title2", "ArtistId")
  op <- options(datatable.allow.cartesian = TRUE)
  withr::defer(options(op))
  expect_true(reference.test(merge(Album, Album2)))

  # When neither x nor y has a key, the merge on common columns.
  Artist <- dbi.table(conn, DBI::Id("Artist"))
  Album <- dbi.table(conn, DBI::Id("Album"))
  expect_true(reference.test(merge(Artist, Album)))

  # A relational merge (merge on foreign keys) should just return x since
  # related tables return NULL.
  Track <- dbi.table(conn, DBI::Id("Track"))
  expect_true(identical(merge(Track), Track))
})


chinook <- dbi.attach(chinook.duckdb)
DBI::dbExecute(chinook, DBI::SQL("SET threads TO 1;"))

# Relational merge (1) finds the internal name of each foreign key column
# in the dbi.table's fields attribute, then (2) finds the first column in the
# dbi.table that matches each internal name exactly. When all columns are found,
# then they are used at the default 'by'.

test_that("relational merge works", {
  g <- Genre[, .(gid = GenreId, Genre.Name = Name)]
  expect_s3_class({
    track_genre <- merge(Track, g)
  }, "dbi.table")

  ref <- merge(as.data.table(Track), as.data.table(g),
               by.x = "GenreId", by.y = "gid")

  expect_true(all.equal(as.data.table(track_genre), ref,
                        ignore.row.order = TRUE))
})

# Merge should fail when a column in the foreign key constraint is
# not in the dbi.table.

test_that("relational merge fails when column missing", {
  track <- Track
  track[, GenreId := NULL]

  # Fail as if 'by' omitted.
  # a non-empty character vector of column names is required for 'by'
  expect_error(merge(track, Genre))
})

test_that("relational merge works", {
  expect_s3_class(x <- merge(Track), "dbi.table")

  track <- as.data.table(Track)

  nm <- names(x)[1:3]
  nm <- substring(nm, 1, nchar(nm) - 2L)

  for (i in rev(nm)) {
    y <- as.data.table(get(i))
    setnames(y, names(y), paste(i, names(y), sep = "."))
    track <- merge(track, y, by.x = paste0(i, "Id"), by.y = names(y)[[1L]],
                   all.x = TRUE)
  }

  expect_true(all.equal(as.data.table(x), track, ignore.row.order = TRUE))
})

test_that("relational merge works with recursive = TRUE", {
  expect_s3_class(x <- merge(Track, recursive = TRUE), "dbi.table")

  track <- as.data.table(Track)

  nm <- names(x)[1:4]
  nm <- substring(nm, 1L, nchar(nm) - 2L)

  for (i in rev(nm)) {
    if (i == "Album.Artist") {
      y <- as.data.table(get("Artist"))
      setnames(y, names(y), paste("Artist", names(y), sep = "."))
    } else {
      y <- as.data.table(get(i))
      setnames(y, names(y), paste(i, names(y), sep = "."))
    }

    track <- merge(track, y, by.x = paste0(i, "Id"), by.y = names(y)[[1L]],
                   all.x = TRUE)
  }

  expect_true(all.equal(as.data.table(x), track, ignore.row.order = TRUE))
})

detach("duckdb:chinook_duckdb")
