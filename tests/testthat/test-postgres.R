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
  expect_no_error(e <- dbi.attach(rnacentral.postgres))
  expect_true(identical(as.environment(2L), e))
  expect_true(setequal(ls(e[["../catalog"]]),
                       c("information_schema", "pg_catalog", "rnacen")))
  expect_silent(detach(2L))
})
