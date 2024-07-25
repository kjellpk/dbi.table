test_that("dbi.table works on data.table help examples", {

  DT <- data.table(x = rep(c("b", "a", "c"), each = 3),
                   y = c(1, 3, 6),
                   v = 1:9)

  conns <- list(DBI::dbConnect(RSQLite::SQLite(), ":memory:"),
                DBI::dbConnect(duckdb::duckdb(), ":memory:"))

  for (conn in conns) {
    expect_no_error({
      DBI::dbWriteTable(conn, "DT", DT)
      DBIT <- dbi.table(conn, DBI::Id("DT"))
    })

    expect_s3_class(DT, "data.table")
    expect_s3_class(DBIT, "dbi.table")

    # identical(dim(DT), dim(DF))    # TRUE
    # identical(DF$a, DT$a)          # TRUE

    expect_true(is.list(DT))
    expect_true(is.list(DBIT))

    expect_true(is.data.frame(DT))
    expect_false(is.data.frame(DBIT))

    # DT[2]                          # 2nd row
    # DT[3:2]                        # 3rd and 2nd row

    expect_true(reference.test(
      DBIT[order(x)],
      ignore.row.order = FALSE,
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[order(x), ],
      ignore.row.order = FALSE,
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[y > 2],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[y > 2 & v > 5],
      verbose = FALSE))

    # DT[!2:4]                       # all rows other than 2:4
    # DT[-(2:4)]                     # same

    # select|compute columns data.table way
    # DT[, v]                        # v column (as vector)

    expect_true(reference.test(
      DBIT[, list(v)],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[, .(v)],
      verbose = FALSE))

    # DT[, sum(v)]                   # sum of column v, returned as vector

    expect_true(reference.test(
      DBIT[, .(sum(v, na.rm = TRUE))],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[, .(sv=sum(v, na.rm = TRUE))],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[, .(v, v*2)],
      verbose = FALSE))


    # subset rows and select|compute data.table way
    # DT[2:3, sum(v)]                # sum(v) over rows 2 and 3, return vector
    # DT[2:3, .(sum(v))]             # same, but return data.table with column V1
    # DT[2:3, .(sv=sum(v))]          # same, but return data.table with column sv
    # DT[2:5, cat(v, "\n")]          # just for j's side effect


    # select columns the data.frame way
    expect_true(reference.test(
      DBIT[, 2],
      verbose = FALSE))

    expect_true(reference.test({
      colNum <- 2
      DBIT[, ..colNum]},
      verbose = FALSE))

    # DT[["v"]]                      # same as DT[, v] but faster if called in a loop


    # grouping operations - j and by

    expect_true(reference.test(
      DBIT[, sum(v, na.rm = TRUE), by = x],
      verbose = FALSE))

  # DT[, sum(v), keyby = x]              # same, but order the result on by cols
  # DT[, sum(v), by = x, keyby = TRUE]   # same, but using sorting flag

    expect_true(reference.test(
      DBIT[, sum(v, na.rm = TRUE), by = x][order(x)],
      ignore.row.order = FALSE,
      verbose = FALSE))

  # fast ad hoc row subsets (subsets as joins)
  # DT["a", on="x"]                # same as x == "a" but uses binary search (fast)
  # DT["a", on=.(x)]               # same, for convenience, no need to quote every column
  # DT[.("a"), on="x"]             # same

  # NOTE: not optimized in dbi.table but still useful as tests

    expect_true(reference.test(
      DBIT[x == "a"],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[x != "b" | y != 3],
      verbose = FALSE))

  # DT[.("b", 3), on=c("x", "y")]  # join on columns x,y of DT; uses binary search (fast)
  # DT[.("b", 3), on=.(x, y)]      # same, but using on=.()
  # DT[.("b", 1:2), on=c("x", "y")]             # no match returns NA
  # DT[.("b", 1:2), on=.(x, y), nomatch=NULL]   # no match row is not returned
  # DT[.("b", 1:2), on=c("x", "y"), roll=Inf]   # locf, nomatch row gets rolled by previous row
  # DT[.("b", 1:2), on=.(x, y), roll=-Inf]      # nocb, nomatch row gets rolled by next row
  # DT["b", sum(v*y), on="x"]                   # on rows where DT$x=="b", calculate sum(v*y)

  # all together now
  ####DT[x!="a", sum(v), by=x]                    # get sum(v) by "x" for each i != "a"
  # DT[!"a", sum(v), by=.EACHI, on="x"]         # same, but using subsets-as-joins
  # DT[c("b","c"), sum(v), by=.EACHI, on="x"]   # same
  # DT[c("b","c"), sum(v), by=.EACHI, on=.(x)]  # same, using on=.()

  # joins as subsets
    expect_no_error({
      X <- data.table(x = c("c", "b"), v = 8:7, foo = c(4, 2))
      DBI::dbWriteTable(conn, "X", X)
      X <- dbi.table(conn, "X")
    })

    expect_true(reference.test(
      DBIT[X, on = "x"],
      verbose = FALSE))

    expect_true(reference.test(
      X[DBIT, on = "x"],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = "x", nomatch = NULL],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[!X, on = "x"],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = c(y = "v")],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = "y == v"],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = .(y <= foo)],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = "y <= foo"],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = c("y <= foo")],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = .(y >= foo)],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, on = .(x, y <= foo)],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[X, .(x, y, x.y, v), on = .(x, y >= foo)],
      verbose = FALSE))

  # NOTE: mult and .EACHI not supported in version 1

  # DT[X, on="x", mult="first"]           # first row of each group
  # DT[X, on="x", mult="last"]            # last row of each group
  # DT[X, sum(v), by=.EACHI, on="x"]      # join and eval j for each row in i
  # DT[X, sum(v)*foo, by=.EACHI, on="x"]  # join inherited scope
  # DT[X, sum(v)*i.v, by=.EACHI, on="x"]  # 'i,v' refers to X's v column
  # DT[X, on=.(x, v>=v), sum(y)*foo, by=.EACHI] # NEW non-equi join with by=.EACHI (v1.9.8+)



  # setting keys
  # kDT = copy(DT)                        # (deep) copy DT to kDT to work with it.
  # setkey(kDT,x)                         # set a 1-column key. No quotes, for convenience.
  # setkeyv(kDT,"x")                      # same (v in setkeyv stands for vector)
  # v="x"
  # setkeyv(kDT,v)                        # same
  # haskey(kDT)                           # TRUE
  # key(kDT)                              # "x"


  # NOTE: keys are not supported

  # fast *keyed* subsets
  # kDT["a"]                              # subset-as-join on *key* column 'x'
  # kDT["a", on="x"]                      # same, being explicit using 'on=' (preferred)

  # all together
  # kDT[!"a", sum(v), by=.EACHI]          # get sum(v) for each i != "a"

  # multi-column key
  # setkey(kDT,x,y)                       # 2-column key
  # setkeyv(kDT,c("x","y"))               # same

  # fast *keyed* subsets on multi-column key
  # kDT["a"]                              # join to 1st column of key
  # kDT["a", on="x"]                      # on= is optional, but is preferred
  # kDT[.("a")]                           # same, .() is an alias for list()
  # kDT[list("a")]                        # same
  # kDT[.("a", 3)]                        # join to 2 columns
  # kDT[.("a", 3:6)]                      # join 4 rows (2 missing)
  # kDT[.("a", 3:6), nomatch=NULL]        # remove missing
  # kDT[.("a", 3:6), roll=TRUE]           # locf rolling join
  # kDT[.("a", 3:6), roll=Inf]            # same as above
  # kDT[.("a", 3:6), roll=-Inf]           # nocb rolling join
  # kDT[!.("a")]                          # not join
  # kDT[!"a"]                             # same

  # more on special symbols, see also ?"special-symbols"
  # DT[.N]                                  # last row

  # Note: .N only supported in j

    expect_no_error(DBIT[, .N]) # dbi.table returns dbi.table,
                                # data.table returns numeric

    expect_true(reference.test(
      DBIT[, .N, by = x],
      verbose = FALSE))

  # DT[, .SD, .SDcols=x:y]                  # select columns 'x' through 'y'
  # DT[ , .SD, .SDcols = !x:y]              # drop columns 'x' through 'y'
  # DT[ , .SD, .SDcols = patterns('^[xv]')] # select columns matching '^x' or '^v'
  # DT[, .SD[1]]                            # first row of all columns
  # DT[, .SD[1], by=x]                      # first row of 'y' and 'v' for each group in 'x'
  # DT[, c(.N, lapply(.SD, sum)), by=x]     # get rows *and* sum columns 'v' and 'y' by group
  # DT[, .I[1], by=x]                       # row number in DT corresponding to each group
  # DT[, grp := .GRP, by=x]                 # add a group counter column
  # DT[ , dput(.BY), by=.(x,y)]             # .BY is a list of singletons for each group
  # X[, DT[.BY, y, on="x"], by=x]           # join within each group
  # DT[, {
  #   # write each group to a different file
  #   fwrite(.SD, file.path(tempdir(), paste0('x=', .BY$x, '.csv')))
  # }, by=x]
  # dir(tempdir())

  # add/update/delete by reference (see ?assign)

    expect_true(reference.test({
      DBIT[, z := 42L]
      DBIT[]
      },
      verbose = FALSE))

    expect_true(reference.test({
      DBIT[, z := NULL]
      DBIT[]
      },
      verbose = FALSE))

  # print(DT["a", v:=42L, on="x"])        # subassign to existing v column by reference
  # print(DT["b", v2:=84L, on="x"])       # subassign to new column by reference (NA padded)

    expect_true(reference.test({
      DBIT[, m := mean(v), by = x]
      DBIT[]
      },
      verbose = FALSE))


    expect_no_error(rm(DBIT))


  # advanced usage
    expect_no_error({
      DT <- data.table(x = rep(c("b", "a", "c"), each = 3),
                      v = c(1, 1, 1, 2, 2, 1, 1, 2, 2),
                      y = c(1, 3, 6),
                      a = 1:9,
                      b = 9:1)
      DBI::dbWriteTable(conn, "DT", DT, overwrite = TRUE)
      DBIT <- dbi.table(conn, "DT")
    })

    expect_true(reference.test(
      DBIT[, sum(v, na.rm = TRUE), by = .(y %% 2)],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[, sum(v, na.rm = TRUE), by = .(bool = y %% 2)],
      verbose = FALSE))

  # DT[, .SD[2], by = x]                    # get 2nd row of each group
  # DT[, tail(.SD, 2), by = x]               # last 2 rows of each group
  # DT[, lapply(.SD, sum), by = x]          # sum of all (other) columns for each group
  # DT[, .SD[which.min(v)], by = x]         # nested query by group

    expect_true(reference.test(
      DBIT[, list(MySum = sum(v, na.rm = TRUE),
                  MyMin = min(v, na.rm = TRUE),
                  MyMax = max(v, na.rm = TRUE)),
          by = .(x, y %% 2)],
      verbose = FALSE))

  # DT[, .(a = .(a), b = .(b)), by=x]     # list columns
  # DT[, .(seq = min(a):max(b)), by=x]    # j is not limited to just aggregations

    expect_true(reference.test(
      DBIT[, sum(v, na.rm = TRUE), by = x][V1 < 20],
      verbose = FALSE))

    expect_true(reference.test(
      DBIT[, sum(v, na.rm = TRUE), by = x][order(-V1)],
      verbose = FALSE))

  # DT[, c(.N, lapply(.SD, sum)), by = x]    # get number of observations and sum per group
  # DT[, {tmp <- mean(y);
  #       .(a = a - tmp, b = b - tmp)
  #       }, by = x]                        # anonymous lambda in 'j', j accepts any valid
                                            # expression. TO REMEMBER: every element of
                                            # the list becomes a column in result.

    expect_no_error(DBI::dbDisconnect(conn))
  }
})
