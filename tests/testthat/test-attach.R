test_that("dbi.attach works", {
  dbs <- list(SQLite = chinook.sqlite,
              duckdb = chinook.duckdb)

  for (d in dbs) {
    expect_no_error(dbi.attach(d, pos = 2L))
    expect_type(e <- as.environment(2L), type = "environment")
    expect_s3_class(e[["../catalog"]], "dbi.catalog")
    expect_vector(s <- ls(e), ptype = character())
    expect_true(all(unlist(eapply(e, is.dbi.table))))
    expect_warning(eval(quote(Album[, z := 42]), envir = e))
    expect_silent(detach(2L))
  }

  skip_on_cran()
  #skip_if_offline() #Borks in github Actions, adding curl package didn't help

  rna <- function() {
    DBI::dbConnect(RPostgres::Postgres(),
                   user = "reader",
                   password = "NWDMCE5xdipIjRrp",
                   host = "hh-pgsql-public.ebi.ac.uk",
                   port = 5432L,
                   dbname = "pfmegrnargs")
  }

  expect_no_error(e <- dbi.attach(rna)) #works b/c only 1 schema
  expect_true(identical(as.environment(2L), e))
  expect_equal(length(ls(e[["../catalog"]])), 2L)
  expect_true("information_schema" %chin% ls(e[["../catalog"]]))
  expect_true("rnacen" %chin% ls(e[["../catalog"]]))
  expect_silent(detach(2L))

  expect_no_error(e <- dbi.attach(rna, schema = "rnacen")) #works b/c only 1 schema
  expect_true(identical(as.environment(2L), e))
  expect_equal(length(ls(e[["../catalog"]])), 2L)
  expect_true("information_schema" %chin% ls(e[["../catalog"]]))
  expect_true("rnacen" %chin% ls(e[["../catalog"]]))
  expect_silent(detach(2L))

  rdo <- function() {
    DBI::dbConnect(RMariaDB::MariaDB(),
                   user = "guest",
                   password = "ctu-relational",
                   host = "relational.fel.cvut.cz",
                   port = 3306)
  }

  expect_error(e <- dbi.attach(rdo)) #since not interactive / multiple schemas
  expect_no_error(e <- dbi.attach(rdo, schema = "Chinook"))
  expect_true(identical(as.environment(2L), e))
  expect_equal(length(ls(e[["../catalog"]])), 2L)
  expect_true("information_schema" %chin% ls(e[["../catalog"]]))
  expect_true("Chinook" %chin% ls(e[["../catalog"]]))
  expect_silent(detach(2L))
})
