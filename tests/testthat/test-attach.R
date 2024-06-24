test_that("dbi.attach works", {
  dbs <- list(SQLite = chinook.sqlite,
              duckdb = chinook.duckdb)

  for (d in dbs) {
    expect_no_error(dbi.attach(d))
    expect_type(e <- as.environment(2L), type = "environment")
    expect_s3_class(e[["..catalog"]], "dbi.catalog")
    expect_vector(s <- ls(e), ptype = character())
    expect_true(all(unlist(eapply(e, is.dbi.table))))
    expect_error(eval(quote(Album[, z := 42]), envir = e))
    expect_silent(detach(2L))
  }
})
