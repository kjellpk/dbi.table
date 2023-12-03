# Compare with reference result. Test the square bracket operator ([]) on a
# single dbi.table.  Example: 'Album[AlbumId < 7 & ArtistId == 2, .(Title)]'
#
#  ## <Chinook_Sqlite> Album 
#  ##             Title
#  ## Balls to the Wall
#  ## Restless and Wild
#
# This test evaluates the input two different ways:
#
#   1. (the typical dbi.table workflow) the input is translated to sql,
#      evaluated by the connected database, and the results are downloaded to
#      a data.table.
#
#   2. (reference) the entire database table is downloaded to a data.table then
#      the square bracket call is computed locally.
#
# The goal of the dbi.table package is to match the reference answer.  Right now
# matching up to row order is acceptable.


compare_with_data.table <- function(x, env = parent.frame(),
                                    ignore.row.order = TRUE,
                                    verbose = FALSE) {
  x <- substitute(x)
  DBIT_name <- as.character(x[[2]])

  check_env <- new.env()
  check_env[[DBIT_name]] <- eval(x[[2]], envir = env)

  nms <- setdiff(all.vars(x), DBIT_name)
  nms <- setdiff(nms, names(check_env[[DBIT_name]]))

  for (nm in nms)
    check_env[[nm]] <- env[[nm]]

  DBIT <- as.data.table(eval(x, envir = check_env))

  check_env[[DBIT_name]] <- as.data.table(check_env[[DBIT_name]])

  DT <- eval(x, envir = check_env)

  check <- data.frame(target_names = names(DT),
                      target_types = vapply(DT, typeof, ""),
                      current_names = names(DBIT),
                      current_types = vapply(DBIT, typeof, ""))
  check$check <- (check$target_names == check$current_names) &
                   (check$target_types == check$current_types)

  eq <- all.equal(DT, DBIT, ignore.row.order = ignore.row.order)

  if (verbose) {
    cat("\n")
    print(x)
    cat("\n")

    print(check)
    cat("\n")

    print(eq)
    cat("\n")
  }

  isTRUE(eq)
}
