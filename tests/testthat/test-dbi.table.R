compare_with_data.table <- function(x, env = parent.frame(),
                                    ignore.row.order = TRUE) {
  x <- substitute(x)

  stopifnot(is.dbi.table(env[[DBIT_name <- as.character(x[[2]])]]))

  env_DT <- rlang::env_clone(env)
  assign(DBIT_name, as.data.table(env[[DBIT_name]]), envir = env_DT)

  DBIT <- as.data.table(eval(x, envir = env))
  DT <- eval(x, envir = env_DT)

  check <- data.frame(target_names = names(DT),
                      target_types = vapply(DT, typeof, ""),
                      current_names = names(DBIT),
                      current_types = vapply(DBIT, typeof, ""))
  check$check <- (check$target_names == check$current_names) &
                   (check$target_types == check$current_types)
  cat("\n")
  print(x)
  cat("\n")

  print(check)
  cat("\n")

  print(eq <- all.equal(DT, DBIT, ignore.row.order = ignore.row.order))
  cat("\n")

  isTRUE(eq)
}



test_that("", {
  expect_no_error(dbi.attach(ex_chinook))
  expect_s3_class(Album <- Album, "dbi.table")
  expect_true(compare_with_data.table(Album[AlbumId > 7 & AlbumId < 13]))
})
