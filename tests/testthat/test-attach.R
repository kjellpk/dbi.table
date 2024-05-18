test_that("dbi.attach works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_type(e <- as.environment("RSQLite:chinook_sqlite"),
              type =  "environment")
  expect_s3_class(e, "dbi_database")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))
  expect_error(eval(quote(Album[, z := 42]), envir = e))
  expect_silent(detach("RSQLite:chinook_sqlite"))
})
