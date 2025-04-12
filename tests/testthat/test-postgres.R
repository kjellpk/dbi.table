rnacentral.postgres <- function() {
  DBI::dbConnect(RPostgres::Postgres(),
                 user = "reader",
                 password = "NWDMCE5xdipIjRrp",
                 host = "hh-pgsql-public.ebi.ac.uk",
                 port = 5432L,
                 dbname = "pfmegrnargs")
}

test_that("dbi.attach works on Postgres w/ schema arg", {
  skip_on_cran()
  expect_no_error(e <- dbi.attach(rnacentral.postgres, schema = "rnacen"))
  expect_true(identical(as.environment(2L), e))
  expect_equal(length(ls(e[["../catalog"]])), 2L)
  expect_true("information_schema" %in% ls(e[["../catalog"]]))
  expect_true("rnacen" %in% ls(e[["../catalog"]]))
  expect_silent(detach(2L))
})
