test_that("dbi.attach works", {
  expect_no_error(dbi.attach(chinook.sqlite))
  expect_type(e <- as.environment("RSQLite:Chinook_Sqlite"),
              type =  "environment")
  expect_vector(s <- ls(e), ptype = character())
  expect_true(all(unlist(eapply(e, is.dbi.table))))
  expect_silent(detach("RSQLite:Chinook_Sqlite"))
})
