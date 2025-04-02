ctu.mariadb <- function() {
  DBI::dbConnect(RMariaDB::MariaDB(),
                 user = "guest",
                 password = "ctu-relational",
                 host = "relational.fel.cvut.cz",
                 port = 3306)
}

test_that("dbi.attach works on MariaDB / needs schema arg", {
  skip_on_cran()
  #since not interactive / multiple schemas
  expect_error(e <- dbi.attach(ctu.mariadb))
  expect_no_error(e <- dbi.attach(ctu.mariadb, schema = "Chinook"))
  expect_true(identical(as.environment(2L), e))

  catalog <- e[["../catalog"]]
  expect_equal(length(ls(catalog)), 2L)
  expect_true("information_schema" %in% ls(catalog))
  expect_true("Chinook" %in% ls(catalog))

  # When a connection is read only, then temporary tables all have to be
  # in query. When there are more than 500 rows, dbi.tables tries
  # to use a temporary table first, then falls back to in query.
  # This test really belongs in test-as.data.table.R but is here b/c it
  # needs a read only connection.

  iris <- datasets::iris
  iris$Species <- as.character(iris$Species)
  big_iris <- iris[rep(seq_len(nrow(iris)), each = 5), ]
  row.names(big_iris) <- NULL
  expect_null(catalog$.temporary_table_denied)
  expect_warning(x <- as.dbi.table(catalog, big_iris))
  expect_true(catalog$.temporary_table_denied)
  expect_identical(as.data.frame(x, n = -1L), big_iris)

  expect_silent(detach(2L))
})
