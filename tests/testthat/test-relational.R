# When the information schema is "bare bones" or doesn't include the
# 'referential_constraints' view, then merge should work like an unkeyed
# data.table.

dbi.attach(chinook.sqlite)


test_that("merge works on shared column (ArtistId)", {
  expect_true(reference.test(merge(Album, Artist)))
})


test_that("merge fails when relational data not available", {
  x <- Artist[, .(id = ArtistId, Name)]

  # non-empty character vectors of column names are required
  # for 'by.x' and 'by.y'
  expect_error(merge(Album, x))

  # A non-empty vector of column names for `by` is required.
  expect_error(merge(as.data.table(Album), as.data.table(x)))

  # A relational merge (merge on foreign keys) should just return x since
  # related tables return NULL.
  expect_true(identical(merge(Track), Track))
})


detach("RSQLite:chinook_sqlite")
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
