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
  expect_equal(length(ls(e[["../catalog"]])), 2L)
  expect_true("information_schema" %in% ls(e[["../catalog"]]))
  expect_true("Chinook" %in% ls(e[["../catalog"]]))
  expect_silent(detach(2L))
})
